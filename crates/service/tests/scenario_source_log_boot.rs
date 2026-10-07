//! Composed boot scenario for the read-once source-log walk (#572): one locked whole-file walk
//! plus one bounded suffix walk, publication only after success, the gated repair of a torn
//! final frame, refusal of a truncation into the recorded prefix, a typed boot failure on poison
//! followed by restart recovery, the handoff drift check, and the not-installed fallbacks.
#![cfg(feature = "scenario")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::duplicate_mod
)]

mod support;

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use pe_core_types::{
    BasisPoints, CollateralAmount, EventSeq, MarketId, OutcomeId, ReceivedAt,
    ReconstructionQuality, SourceId, SourceTimestamp, SourceTradeId, VenueMarketId, WalletAddress,
};
use pe_event_log::{AppendReceipt, ContentType, EnvelopeIn, Scanner, Writer};
use pe_execution_core::LiveJournal;
use pe_paper_state::{FillRecord, MigrationMetadata, MigrationPhase, PaperStateDb};
use pe_service::activity_ingest::ACTIVITY_WS_SOURCE_ID;
use pe_service::paper_migration::{PaperMigrationBoot, PaperMigrationPaths};
use pe_service::paper_recovery::{
    CanonicalFillResult, ExpectedAuthority, FinancialPayload, FinancialResult,
    PaperFillOperationIdentity, PaperLogFrame, PaperLogRecord, QualificationStarted, TailBinding,
    paper_era, replay_membership, scan_paper_log,
};
use pe_service::risk_inputs::SourceReceiptIndex;
use pe_service::source_log_boot::{SourceLogBoot, SourceLogBootHooks};
use pe_service::supabase_state::PreparedFillRequest;
use pe_service::trade_poller::{DAILY_BOUNDARY_SOURCE_ID, rebuild_reconciliation_obligations};
use pe_source_polymarket_public::{ACTIVITY_PARSER_VERSION, ACTIVITY_SCHEMA_VERSION};
use pe_trader_index::{Watchlist, WatchlistEntry, WatchlistTier};
use rusqlite::Connection;
use rust_decimal::Decimal;
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

fn append(path: &Path, envelope_in: EnvelopeIn) -> AppendReceipt {
    let mut writer = Writer::open(path).unwrap();
    writer.append_synced(envelope_in).unwrap()
}

fn append_paper_record(path: &Path, record: &PaperLogRecord) -> AppendReceipt {
    append(
        path,
        envelope(
            "pe-service.paper",
            2,
            1,
            &serde_json::to_vec(record).unwrap(),
            NOW_UNIX,
        ),
    )
}

fn empty_tail() -> TailBinding {
    TailBinding {
        physical_tail: 5,
        last_sequence: None,
        last_hash: "00".repeat(32),
    }
}

fn start_record() -> PaperLogRecord {
    PaperLogRecord::QualificationStarted(Arc::new(QualificationStarted {
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
    (dir, install_generation(paths))
}

/// Install the version-two generation for `paths`, recording `paths.binary_identity` as the
/// activating binary.
fn install_generation(paths: PaperMigrationPaths) -> PaperMigrationPaths {
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
    paths
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
                    // Coalescing scans a wallet's distinct seconds. One wallet holds thousands in
                    // a production month, not 20,000, so 100 trades share each second.
                    NOW_UNIX + 1 + index / 100,
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
    install_committed_open_read_for_wallet(
        paper,
        source_log,
        WalletAddress::from_hex(WALLET).unwrap(),
    );
}

fn install_committed_open_read_for_wallet(
    paper: &Arc<PaperStateDb>,
    source_log: &Path,
    wallet: WalletAddress,
) {
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
        "proxyWallet": wallet, "timestamp": NOW_UNIX + 25_000,
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
        counterpart_basis_receipt: None,
        frame_admission_receipt: None,
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
        let sidecar = paths.source_log.with_extension("log.boot-checkpoint");
        let sidecar_before = std::fs::read(&sidecar).ok();
        let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_pe-service"));
        command
            .env_clear()
            .arg("--prepare-source-checkpoint")
            .arg("--paper-state")
            .arg(&paths.fixed_main);
        let output = support::bounded_command_output(command).await;
        let stderr = String::from_utf8(output.stderr).unwrap();
        assert!(!output.status.success(), "{stderr}");
        assert!(
            stderr.contains("validate open decision continuations before deployment"),
            "{stderr}"
        );
        assert!(
            stderr.contains(&format!("open decision continuation {id}:")),
            "{stderr}"
        );
        assert_eq!(std::fs::read(&sidecar).ok(), sidecar_before);
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

/// #628: the boot bankroll gate through the real binary, after a Start.
///
/// `main` decides per store whether the Start baseline still binds: the local store by its own
/// fill count, the authority by its own progress marker (`paper_bankroll.last_prepared_seq`). The
/// two legitimately disagree in production — the authority commits a fill before the local
/// projection catches up — and before this gate every post-fill boot refused with the
/// frozen-baseline message. Exercising the helper with literal booleans could not catch a `main`
/// that ignored the markers, so these cases boot the binary itself with `--exit-after-anchors`,
/// which runs past both guards, the Start seed and the frame reconciliation before exiting.
///
/// The successful fill cases share ONE genuine Prepared fill, laid down exactly as the runtime
/// leaves it before the authority answers (the causal source frame, then the Prepared frame —
/// `orchestrator.rs` synchronizes Prepared BEFORE calling the authority). They differ only in how
/// far past that point the crash happened, so the admitted "authority ahead" state is not merely
/// tolerated: the boot has to recover it.
enum PostStartBootCase {
    /// A synchronized full rerank removes the Start wallet and boots an empty generation.
    EmptyPostStart,
    /// A post-Start capacity record keeps structure, then the boot fence filter empties live.
    FilteredPostStart,
    /// An empty Start membership without a post-Start structural record remains refused.
    EmptyStartPinned,
    /// Crashed after the authority applied the fill and before the local projection: the store
    /// still carries the baseline, the authority no longer does. Both must be admitted, and the
    /// boot must then recover the fill locally and append its Final.
    AuthorityAheadLocalUntouched,
    /// Crashed after the local projection and the Final append: both stores carry the fill, and
    /// the boot has nothing left to reconcile.
    BothTraded,
    /// An untraded local store reset onto the wrong balance is still the fresh-activation
    /// refusal, before the authority is consulted.
    LocalWrongBaseline,
    /// An untraded authority (null progress) on the wrong balance is still refused, before any
    /// Start seed reaches it.
    AuthorityWrongBaseline,
}

async fn post_start_boot_bankroll_case(case: PostStartBootCase) {
    post_start_boot_bankroll_case_with_checkpoint_parity(case, false).await;
}

async fn post_start_boot_bankroll_case_with_checkpoint_parity(
    case: PostStartBootCase,
    checkpoint_parity: bool,
) {
    use axum::{Json, Router, extract::State, http::Uri, routing::any};
    use std::sync::Mutex;

    #[derive(Clone)]
    struct Authority {
        requests: Arc<Mutex<Vec<Uri>>>,
        bankroll: serde_json::Value,
        start: AppendReceipt,
        /// The `commit_fill_v2` answer for the Prepared fixture, when one exists.
        commit: Option<serde_json::Value>,
        progress: Arc<Mutex<(Decimal, Option<u64>)>>,
    }
    async fn respond(
        State(authority): State<Authority>,
        uri: Uri,
        body: axum::body::Bytes,
    ) -> Json<serde_json::Value> {
        use serde_json::json;
        authority.requests.lock().unwrap().push(uri.clone());
        let query = uri.query().unwrap_or_default();
        Json(match uri.path() {
            "/rest/v1/service_config" => {
                // Financial15 rows: the two legacy-only keys (`fill_mode`, `polymarket_fee_rate`)
                // are unknown after Start and would refuse the snapshot.
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
                ];
                json!(rows.map(|(key, value, value_type)| json!({"key": key, "value": value, "value_type": value_type})))
            }
            "/rest/v1/ranking_entries" => {
                assert!(query.contains("batch_id=eq.572"), "{uri}");
                json!([
                    {"batch_id":572,"rank":1,"wallet_hex":WALLET,"ls_tstat":"3","hit_rate":"0.6","n_trades":20,"last_trade_unix":10,"survives":true}
                ])
            }
            "/rest/v1/paper_bankroll" => {
                // One observation carries both the balance and the progress marker.
                assert!(
                    query.contains("select=bankroll_str,last_prepared_seq"),
                    "{uri}"
                );
                json!([authority.bankroll])
            }
            "/rest/v1/rpc/seed_financial_start" => json!({
                "outcome": "existing",
                "start_seq": authority.start.sequence.0,
                "start_hash": authority.start.this_hash.to_hex().to_string(),
            }),
            "/rest/v1/rpc/commit_fill_v2" => {
                let request: serde_json::Value = serde_json::from_slice(&body).unwrap();
                let existing = authority.commit.as_ref().unwrap();
                if request["p_prepared_seq"] == existing["applied_prepared_seq"] {
                    existing.clone()
                } else {
                    let mut progress = authority.progress.lock().unwrap();
                    assert_eq!(request["p_expected_prior_seq"], json!(progress.1));
                    let principal: Decimal =
                        request["p_principal"].as_str().unwrap().parse().unwrap();
                    let fee: Decimal = request["p_fee"].as_str().unwrap().parse().unwrap();
                    progress.0 -= principal + fee;
                    progress.1 = request["p_prepared_seq"].as_u64();
                    json!({
                        "outcome": "applied", "bankroll": progress.0.to_string(),
                        "applied_prepared_seq": progress.1,
                        "row": {
                            "idempotency_key": request["p_idempotency_key"],
                            "leader_wallet": request["p_leader_wallet"],
                            "source_trade_id": request["p_source_trade_id"],
                            "market_id": request["p_market_id"], "outcome_id": request["p_outcome_id"],
                            "side": request["p_side"], "quantity": request["p_quantity"],
                            "fill_price": request["p_fill_price"], "principal": request["p_principal"],
                            "fee": request["p_fee"], "entry_unix": request["p_entry_unix"],
                            "prepared_seq": request["p_prepared_seq"]
                        }
                    })
                }
            }
            _ => panic!("unexpected post-Start boot request {uri}"),
        })
    }

    let (dir, mut paths) = version_one_fixture();
    paths.binary_identity = pe_service::build_info::embedded()
        .source_revision
        .to_owned();
    let paths = install_generation(paths);
    let wallet = WalletAddress::from_hex(WALLET).unwrap();
    let mut started = start_record();
    if matches!(case, PostStartBootCase::EmptyStartPinned) {
        let PaperLogRecord::QualificationStarted(ref mut start) = started else {
            unreachable!()
        };
        Arc::make_mut(start).membership.clear();
    }
    let start = append_paper_record(&paths.paper_log, &started);
    let preserved_seal = checkpoint_parity.then(|| {
        append_paper_record(
            &paths.paper_log,
            &PaperLogRecord::QualificationSealed(Box::new(
                pe_service::paper_recovery::QualificationSealed {
                    start_receipt: start,
                    source_prefix: TailBinding::from(&Scanner::verify(&paths.source_log).unwrap()),
                    financial_prefix: TailBinding::from(
                        &Scanner::verify(&paths.paper_log).unwrap(),
                    ),
                    live_prefix: TailBinding::from(&Scanner::verify(&paths.live_journal).unwrap()),
                    decision_evidence_digest: blake3::hash(b"[]").to_hex().to_string(),
                    sealed_cutoff_unix: NOW_UNIX,
                    reason: pe_service::paper_recovery::SealReason::InsufficientEvidence(
                        "synthetic historical closed era".to_owned(),
                    ),
                },
            )),
        )
    });
    let paper = PaperStateDb::open(&paths.fixed_main).unwrap();
    // A reusable anchor keeps the boot bracket off the network: the case is about the bankroll
    // gate, not the venue walk, and `--exit-after-anchors` exits right after the reuse census.
    let now_unix = OffsetDateTime::now_utc().unix_timestamp();
    paper
        .record_reconciled_history_status(&pe_paper_state::WalletHistoryStatusRecord {
            wallet,
            complete: true,
            proof_json: "{\"history\":true}".to_owned(),
            updated_at_unix: now_unix,
        })
        .unwrap();
    support::install_full_history_anchor(&paper, wallet, now_unix);
    // The offline Start resets the local store onto the baseline; the wrong-balance case models a
    // reset onto the wrong amount.
    let local_reset = match case {
        PostStartBootCase::LocalWrongBaseline => dec!(9),
        _ => dec!(10),
    };
    paper
        .reset_financial_era(
            start,
            CollateralAmount::from_decimal_exact(local_reset).unwrap(),
        )
        .unwrap();
    let mut authority_progress =
        serde_json::json!({"bankroll_str": "9", "last_prepared_seq": null});
    let mut commit = None;
    let mut expected_cash = local_reset;
    let prepared = match case {
        PostStartBootCase::AuthorityAheadLocalUntouched | PostStartBootCase::BothTraded => {
            let source_receipt = append(
                &paths.source_log,
                activity_envelope("0xboot-fill", NOW_UNIX + 1),
            );
            let expected = ExpectedAuthority {
                qualification_start_receipt: start,
                prior_completed_prepared_sequence: None,
            };
            let operation = PaperFillOperationIdentity {
                leader_wallet: wallet,
                source_trade_id: SourceTradeId("g2:boot-fill".to_owned()),
                observed_at_bucket: NOW_UNIX,
            };
            let economic = support::economic_prepared(source_receipt, start);
            let prepared = append_paper_record(
                &paths.paper_log,
                &PaperLogRecord::FinancialPrepared {
                    expected_authority: expected.clone(),
                    payload: FinancialPayload::Fill {
                        operation: operation.clone(),
                        economic: economic.clone(),
                    },
                },
            );
            let request =
                PreparedFillRequest::from_prepared(expected, prepared, &operation, &economic);
            // The authority has applied it either way: cash 9, progress at this Prepared.
            authority_progress = serde_json::json!({
                "bankroll_str": "9",
                "last_prepared_seq": prepared.sequence.0,
            });
            expected_cash = dec!(9);
            // Its idempotent `commit_fill_v2` answer, byte-for-byte what the reconciler validates
            // against the Prepared it recomputes.
            commit = Some(serde_json::json!({
                "outcome": "existing",
                "bankroll": "9",
                "applied_prepared_seq": prepared.sequence.0,
                "row": {
                    "idempotency_key": request.idempotency_key,
                    "leader_wallet": request.leader_wallet.to_string(),
                    "source_trade_id": request.source_trade_id.0,
                    "market_id": request.market_id,
                    "outcome_id": request.outcome_id,
                    "side": "buy",
                    "quantity": request.quantity.to_decimal().to_string(),
                    "fill_price": request.fill_price.0.to_string(),
                    "principal": request.principal.to_decimal().to_string(),
                    "fee": request.fee.to_decimal().to_string(),
                    "entry_unix": request.entry_unix,
                    "prepared_seq": prepared.sequence.0,
                },
            }));
            if matches!(case, PostStartBootCase::BothTraded) {
                // Exactly what `apply_financial_result` stores: the fill row, the debited cash and
                // the causal time read back from the source receipt's envelope (NOW_UNIX + 1).
                paper
                    .apply_financial_fill(
                        start,
                        None,
                        prepared.sequence,
                        source_receipt,
                        NOW_UNIX + 1,
                        &FillRecord {
                            idempotency_key: request.idempotency_key.clone(),
                            market_id: MarketId(VenueMarketId(request.market_id.clone())),
                            outcome_id: OutcomeId(request.outcome_id),
                            side: request.side,
                            quantity: request.quantity,
                            fill_price: request.fill_price,
                            principal: request.principal,
                            fee: request.fee,
                        },
                        dec!(9),
                    )
                    .unwrap();
                append_paper_record(
                    &paths.paper_log,
                    &PaperLogRecord::FinancialFinal {
                        prepared_receipt: prepared,
                        result: FinancialResult::Fill {
                            canonical: CanonicalFillResult {
                                outcome: "applied".to_owned(),
                                bankroll: dec!(9),
                                applied_prepared_seq: prepared.sequence,
                                quantity: request.quantity,
                                principal: request.principal,
                                fee: request.fee,
                                fill_price: request.fill_price,
                            },
                        },
                    },
                );
                assert_eq!(paper.fills_count().unwrap(), 1);
                assert_eq!(paper.bankroll().unwrap(), Some(dec!(9)));
            } else {
                assert_eq!(paper.fills_count().unwrap(), 0);
                assert_eq!(paper.bankroll().unwrap(), Some(dec!(10)));
            }
            Some(prepared)
        }
        PostStartBootCase::EmptyPostStart
        | PostStartBootCase::FilteredPostStart
        | PostStartBootCase::EmptyStartPinned
        | PostStartBootCase::LocalWrongBaseline
        | PostStartBootCase::AuthorityWrongBaseline => None,
    };
    if matches!(
        case,
        PostStartBootCase::EmptyPostStart
            | PostStartBootCase::FilteredPostStart
            | PostStartBootCase::EmptyStartPinned
    ) {
        authority_progress = serde_json::json!({
            "bankroll_str": "10",
            "last_prepared_seq": null
        });
    }
    if matches!(case, PostStartBootCase::EmptyPostStart) {
        let ranking = append(
            &paths.source_log,
            envelope(
                "pe-service.watchlist-ranking",
                1,
                1,
                &serde_json::to_vec(&serde_json::json!({
                    "batch_id": 573,
                    "entries": []
                }))
                .unwrap(),
                NOW_UNIX,
            ),
        );
        append_paper_record(
            &paths.paper_log,
            &pe_service::paper_recovery::MembershipChange {
                reason: pe_service::paper_recovery::MembershipReason::FullRerank,
                removed: vec![wallet],
                added: Vec::new(),
                capacity: 1,
                ranking_batch_id: Some(573),
                evidence: serde_json::json!({
                    "kind": "full_rerank",
                    "ranking_receipt": ranking,
                    "admission_receipts": []
                }),
            }
            .into_record(),
        );
    }
    if matches!(case, PostStartBootCase::FilteredPostStart) {
        let config = append(
            &paths.source_log,
            envelope(
                "pe-service.watchlist-capacity-config",
                1,
                1,
                &serde_json::to_vec(&serde_json::json!({
                    "generation": 2,
                    "target": 1,
                    "published_entries": start_batch().entries
                }))
                .unwrap(),
                NOW_UNIX,
            ),
        );
        append_paper_record(
            &paths.paper_log,
            &pe_service::paper_recovery::MembershipChange {
                reason: pe_service::paper_recovery::MembershipReason::CapacityChange,
                removed: Vec::new(),
                added: Vec::new(),
                capacity: 1,
                ranking_batch_id: None,
                evidence: serde_json::json!({
                    "kind": "capacity_change",
                    "generation": 2,
                    "config_receipt": config,
                    "admission_receipts": []
                }),
            }
            .into_record(),
        );
        rusqlite::Connection::open(&paths.fixed_main)
            .unwrap()
            .execute(
                "INSERT INTO wallet_fences (wallet_hex, source_trade_id, cause, proof_json, fenced_at_unix) VALUES (?1, 'fixture', 'invalid_mapping', '{}', 1)",
                [wallet.to_string()],
            )
            .unwrap();
    }
    if checkpoint_parity {
        let pending_paper = Arc::new(PaperStateDb::open(&paths.fixed_main).unwrap());
        install_mixed_current_open_continuations(
            &pending_paper,
            &paths.source_log,
            preserved_seal.unwrap(),
        );
    }
    drop(paper);

    let requests = Arc::new(Mutex::new(Vec::new()));
    let financial_progress = Arc::new(Mutex::new((
        dec!(9),
        prepared.map(|receipt| receipt.sequence.0),
    )));
    let authority = Authority {
        progress: financial_progress.clone(),
        requests: requests.clone(),
        bankroll: authority_progress,
        start,
        commit,
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let router = Router::new().fallback(any(respond)).with_state(authority);
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
        // Equal to the Start baseline: the configured-vs-Start equality is not under test here.
        bankroll_usd: "10".to_owned(),
        ..Default::default()
    };
    let config_path = dir.path().join("service.toml");
    std::fs::write(&config_path, toml::to_string(&cfg).unwrap()).unwrap();
    if checkpoint_parity {
        // Restore the exact same initial durable state before each boot. The full walk and
        // checkpoint must recover both the unmatched Prepared and the independent open entry.
        let initial = [&paths.fixed_main, &paths.source_log, &paths.paper_log]
            .map(|path| std::fs::read(path).unwrap());
        let mut expected = None;
        for checkpoint in [false, true] {
            for (path, bytes) in [&paths.fixed_main, &paths.source_log, &paths.paper_log]
                .into_iter()
                .zip(&initial)
            {
                std::fs::write(path, bytes).unwrap();
            }
            requests.lock().unwrap().clear();
            *financial_progress.lock().unwrap() =
                (dec!(9), prepared.map(|receipt| receipt.sequence.0));
            if checkpoint {
                assert_eq!(
                    SourceLogBoot::prepare_checkpoint(&paths.fixed_main)
                        .unwrap()
                        .1,
                    4
                );
            }
            let before = pe_event_log::scan_metrics::decoded_count(&paths.source_log).unwrap();
            let opened = SourceLogBoot::open(&paths, true).unwrap().unwrap();
            let decoded =
                pe_event_log::scan_metrics::decoded_count(&paths.source_log).unwrap() - before;
            assert_eq!(
                decoded == 0,
                checkpoint,
                "the checkpoint must restore the financial-mode prefix"
            );
            let mut boot = opened.boot;
            let mut sink = opened.sink;
            boot.extend(&mut sink).unwrap();
            let paper = Arc::new(PaperStateDb::open(&paths.fixed_main).unwrap());
            let index = boot.receipt_index();
            assert_same_receipts(
                &index,
                &SourceReceiptIndex::replay(&paths.source_log).unwrap(),
                opened.binding.last_sequence.unwrap().0,
            );
            assert_eq!(
                pe_service::bucket_commit::validate_open_continuations(&paper, &index).unwrap(),
                4
            );
            let obligations = boot.obligations(&paper, &paths.paper_log).unwrap();
            let pending_before = paper.decision_pending_history().unwrap();
            drop(sink);
            drop(boot);
            // Drive the production financial reconciler under the fixed terminal clock.
            // The subprocess boot then checks its existing Final; separate bankroll scenarios
            // exercise the subprocess's unmatched-Prepared recovery on its wall clock.
            let authority = pe_service::supabase_state::SupabaseStateClient::new(
                reqwest::Client::new(),
                &cfg.supabase_url,
                "fixture",
                "fixture",
            );
            let writer = pe_service::paper_recovery::PaperLog::open(&paths.paper_log).unwrap();
            pe_service::orchestrator::SCENARIO_TERMINAL_CLOCK
                .scope(
                    OffsetDateTime::from_unix_timestamp(NOW_UNIX + 25_010).unwrap(),
                    pe_service::supabase_state::reconcile_active_financial_frames(
                        &authority,
                        &paper,
                        pe_service::supabase_state::SourceEvidence::Index(&index),
                        &writer,
                    ),
                )
                .await
                .unwrap();
            drop(writer);
            let output = boot_binary(&config_path, true).await;
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(output.status.success(), "{stderr}");
            assert_eq!(
                stderr.contains("\"checkpoint_used\":true"),
                checkpoint,
                "{stderr}"
            );
            assert!(
                requests
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|uri| uri.path() == "/rest/v1/rpc/commit_fill_v2")
            );
            assert_eq!(paper.fills_count().unwrap(), 1);
            assert_eq!(paper.bankroll().unwrap(), Some(dec!(9)));
            assert_eq!(paper.decision_pending_history().unwrap(), pending_before);
            let (_tx, rx) = tokio::sync::mpsc::channel(4);
            let (hooks, prices, books) =
                mixed_continuation_market_evidence(&paths.source_log, &paper);
            let index = SourceReceiptIndex::replay(&paths.source_log).unwrap();
            let (source_handle, source_rx) =
                pe_service::activity_ingest::SourceLogHandle::channel(4);
            let (trigger, _receiver) = tokio::sync::mpsc::channel(1);
            let source_actor = tokio::spawn(
                pe_service::activity_ingest::ActivityIngest::poll_only(
                    pe_service::source_event_sink::SourceEventSink::open(&paths.source_log)
                        .unwrap(),
                    source_rx,
                    trigger,
                    pe_service::health::new_shared_health_with_ws(false, true, 90),
                )
                .with_source_receipt_index(index.clone())
                .run(),
            );
            let clock = OffsetDateTime::from_unix_timestamp(NOW_UNIX + 25_010).unwrap();
            let mids = pe_service::mid_price_cache::MidPriceCache::with_fetcher(
                prices,
                "https://scenario.test".to_owned(),
            )
            .with_source_log(source_handle.clone())
            .with_clock(Arc::new(move || clock));
            let mut orchestrator = support::continuation_orchestrator_with_market_evidence(
                paper.clone(),
                &paths.paper_log,
                WalletAddress([0xcc; 20]),
                rx,
                hooks,
                Some(pe_service::supabase_state::SupabaseStateClient::new(
                    reqwest::Client::new(),
                    &cfg.supabase_url,
                    "fixture",
                    "fixture",
                )),
                mids,
                books,
            )
            .with_source_receipt_index(index.clone());
            orchestrator
                .configure_financial_log_paths(
                    paths.paper_log.clone(),
                    paths.source_log.clone(),
                    pe_service::live_venue_adapter::LiveAdmissionBuilder::new(
                        reqwest::Client::new(),
                        "http://unused.invalid",
                        "http://unused.invalid",
                        source_handle.clone(),
                    ),
                    Arc::new(pe_service::mark_prices::HistoricalMarkAdapter::new(
                        reqwest::Client::new(),
                        "http://unused.invalid",
                        source_handle,
                    )),
                    index,
                )
                .unwrap();
            orchestrator.seal_before_resume("config", 3).await.unwrap();
            pe_service::orchestrator::SCENARIO_TERMINAL_CLOCK
                .scope(
                    OffsetDateTime::from_unix_timestamp(NOW_UNIX + 25_010).unwrap(),
                    orchestrator.resume_pending_before_producers(),
                )
                .await
                .unwrap();
            drop(orchestrator);
            source_actor.abort();
            let _ = source_actor.await;
            assert!(paper.open_decision_pending().unwrap().is_empty());
            for row in paper.decision_pending_history().unwrap() {
                let decision = pe_service::decision_replay::replay_decision_pending(&row).unwrap();
                assert_eq!(
                    decision.post_boundary.financial_semantic_version,
                    if decision.continuation.version() == 6 {
                        2
                    } else {
                        3
                    }
                );
            }
            let receipts = scan_paper_log(&paths.paper_log)
                .unwrap()
                .iter()
                .filter(|frame| {
                    matches!(
                        &frame.frame,
                        PaperLogFrame::Record(
                            PaperLogRecord::QualificationStarted(_)
                                | PaperLogRecord::QualificationSealed(_)
                        )
                    )
                })
                .map(|frame| frame.receipt)
                .collect::<Vec<_>>();
            assert_eq!(receipts, vec![start, preserved_seal.unwrap()]);
            let finals = paper_era(scan_paper_log(&paths.paper_log).unwrap())
                .frames
                .into_iter()
                .filter_map(|frame| match &frame.frame {
                    PaperLogFrame::Record(PaperLogRecord::FinancialFinal {
                        prepared_receipt,
                        result,
                    }) => Some((*prepared_receipt, serde_json::to_value(result).unwrap())),
                    _ => None,
                })
                .collect::<Vec<_>>();
            assert_eq!(finals.len(), 4);
            assert_eq!(paper.fills_count().unwrap(), 4);
            let operations = paper_era(scan_paper_log(&paths.paper_log).unwrap())
                .frames
                .into_iter()
                .filter_map(|frame| match &frame.frame {
                    PaperLogFrame::Record(PaperLogRecord::FinancialPrepared {
                        expected_authority,
                        payload,
                    }) => Some((frame.receipt, expected_authority.clone(), payload.clone())),
                    _ => None,
                })
                .collect::<Vec<_>>();
            assert_eq!(operations.len(), 4);
            let partial = operations
                .iter()
                .find_map(|(_, _, payload)| match payload {
                    FinancialPayload::Fill {
                        operation,
                        economic,
                    } if operation.leader_wallet == WalletAddress([0xce; 20]) => Some(economic),
                    _ => None,
                })
                .unwrap();
            assert_eq!(partial.sizing.principal.to_decimal(), dec!(0.75));
            assert_eq!(partial.sizing.expected_shares.to_decimal(), dec!(1.5));
            assert!(
                paper
                    .decision_pending_history()
                    .unwrap()
                    .iter()
                    .any(|row| row.wallet == WalletAddress([0xcf; 20])
                        && row.terminal_disposition.as_deref() == Some("no_fill"))
            );
            for wallet in [0xcc, 0xcd, 0xce] {
                assert!(
                    paper
                        .decision_pending_history()
                        .unwrap()
                        .iter()
                        .any(|row| row.wallet == WalletAddress([wallet; 20])
                            && row.terminal_disposition.as_deref() == Some("fill"))
                );
            }
            assert_eq!(paper.bankroll().unwrap(), Some(dec!(4.25)));
            assert_eq!(
                paper.bankroll().unwrap(),
                Some(financial_progress.lock().unwrap().0)
            );
            let result = (
                paper.financial_snapshot(NOW_UNIX + 30_000).unwrap(),
                paper.decision_pending_history().unwrap(),
                obligations,
                paper.gate_history().unwrap(),
                finals,
                operations,
            );
            if let Some(expected) = &expected {
                assert_eq!(&result, expected);
            } else {
                expected = Some(result);
            }
        }
        stop.send(()).unwrap();
        server.await.unwrap();
        return;
    }
    let output = boot_binary(&config_path, true).await;
    let stderr = String::from_utf8(output.stderr).unwrap();
    let hit = requests
        .lock()
        .unwrap()
        .iter()
        .map(|uri| uri.path().to_owned())
        .collect::<Vec<_>>();
    let seeded = hit
        .iter()
        .any(|path| path == "/rest/v1/rpc/seed_financial_start");
    let committed = hit.iter().any(|path| path == "/rest/v1/rpc/commit_fill_v2");
    let finals_for = |prepared: AppendReceipt| {
        paper_era(scan_paper_log(&paths.paper_log).unwrap())
            .frames
            .iter()
            .filter(|frame| {
                matches!(
                    &frame.frame,
                    PaperLogFrame::Record(PaperLogRecord::FinancialFinal { prepared_receipt, .. })
                        if *prepared_receipt == prepared
                )
            })
            .count()
    };
    match case {
        PostStartBootCase::EmptyPostStart => {
            assert!(output.status.success(), "{stderr}");
            assert!(
                stderr.contains("replayed post-Start membership is empty"),
                "{stderr}"
            );
            assert!(seeded, "empty post-Start boot did not reach the Start seed");
            assert!(!committed);
            let era = paper_era(scan_paper_log(&paths.paper_log).unwrap());
            let replayed = replay_membership(&era, start_batch(), &paths.source_log)
                .unwrap()
                .unwrap();
            assert!(replayed.post_start_record_replayed);
            assert!(replayed.watchlist.entries.is_empty());
        }
        PostStartBootCase::FilteredPostStart => {
            assert!(output.status.success(), "{stderr}");
            assert!(
                stderr.contains("replayed post-Start membership has zero eligible live wallets"),
                "{stderr}"
            );
            assert!(seeded);
            let era = paper_era(scan_paper_log(&paths.paper_log).unwrap());
            let replayed = replay_membership(&era, start_batch(), &paths.source_log)
                .unwrap()
                .unwrap();
            assert!(replayed.post_start_record_replayed);
            assert_eq!(replayed.watchlist.entries.len(), 1);
        }
        PostStartBootCase::EmptyStartPinned => {
            assert!(!output.status.success(), "{stderr}");
            assert!(
                stderr.contains(
                    "no wallets to copy: the boot membership generation contains no SURVIVING rows"
                ),
                "{stderr}"
            );
            assert!(!seeded);
            let era = paper_era(scan_paper_log(&paths.paper_log).unwrap());
            let replayed = replay_membership(&era, start_batch(), &paths.source_log)
                .unwrap()
                .unwrap();
            assert!(!replayed.post_start_record_replayed);
            assert!(replayed.watchlist.entries.is_empty());
        }
        PostStartBootCase::AuthorityAheadLocalUntouched | PostStartBootCase::BothTraded => {
            assert!(output.status.success(), "{stderr}");
            assert!(seeded, "the boot must reach the Start seed: {hit:?}");
            let prepared = prepared.unwrap();
            // Authority-ahead: the boot must have asked the authority for the Prepared it found
            // unmatched, applied the answer locally and appended the Final. Both-traded: nothing
            // was left to ask, and the one Final is still the one the fixture wrote.
            let recovered = matches!(case, PostStartBootCase::AuthorityAheadLocalUntouched);
            assert_eq!(committed, recovered, "{hit:?}");
            assert_eq!(finals_for(prepared), 1);
            let paper = PaperStateDb::open(&paths.fixed_main).unwrap();
            assert_eq!(paper.financial_start().unwrap(), Some(start));
            assert_eq!(paper.fills_count().unwrap(), 1);
            assert_eq!(
                paper.financial_last_prepared_seq().unwrap(),
                Some(prepared.sequence)
            );
            assert_eq!(paper.bankroll().unwrap(), Some(expected_cash));
        }
        PostStartBootCase::LocalWrongBaseline => {
            assert!(!output.status.success(), "{stderr}");
            assert!(
                stderr.contains(
                    "local bankroll Some(9) differs from QualificationStarted baseline 10"
                ),
                "{stderr}"
            );
            assert!(
                !hit.iter().any(|path| path == "/rest/v1/paper_bankroll") && !seeded,
                "the local refusal must precede every authority financial read: {hit:?}"
            );
        }
        PostStartBootCase::AuthorityWrongBaseline => {
            assert!(!output.status.success(), "{stderr}");
            assert!(
                stderr.contains(
                    "authoritative bankroll Some(9) differs from QualificationStarted baseline 10"
                ),
                "{stderr}"
            );
            assert!(!seeded, "a refused authority must not be seeded: {hit:?}");
        }
    }
    stop.send(()).unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn post_start_boot_admits_an_authority_that_applied_a_fill_before_the_local_projection() {
    post_start_boot_bankroll_case(PostStartBootCase::AuthorityAheadLocalUntouched).await;
}

#[tokio::test]
async fn empty_post_start_membership_passes_both_real_binary_boot_guards() {
    post_start_boot_bankroll_case(PostStartBootCase::EmptyPostStart).await;
}

#[tokio::test]
async fn filtered_post_start_membership_passes_real_binary_boot_live_guard() {
    post_start_boot_bankroll_case(PostStartBootCase::FilteredPostStart).await;
}

#[tokio::test]
async fn empty_start_pinned_membership_is_refused_by_real_binary_boot() {
    post_start_boot_bankroll_case(PostStartBootCase::EmptyStartPinned).await;
}

#[tokio::test]
async fn post_start_boot_admits_both_stores_after_a_fill() {
    post_start_boot_bankroll_case(PostStartBootCase::BothTraded).await;
}

#[tokio::test]
async fn post_start_boot_refuses_an_untraded_local_store_on_the_wrong_balance() {
    post_start_boot_bankroll_case(PostStartBootCase::LocalWrongBaseline).await;
}

#[tokio::test]
async fn post_start_boot_refuses_an_untraded_authority_on_the_wrong_balance() {
    post_start_boot_bankroll_case(PostStartBootCase::AuthorityWrongBaseline).await;
}

enum PreStartBootCase {
    Selected,
    Empty,
    InvalidContinuation,
}

async fn boot_binary(config_path: &Path, exit_after_anchors: bool) -> std::process::Output {
    support::bounded_command_output(boot_binary_command(config_path, exit_after_anchors)).await
}

fn boot_binary_command(config_path: &Path, exit_after_anchors: bool) -> std::process::Command {
    let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_pe-service"));
    command
        .env_clear()
        .current_dir(config_path.parent().unwrap())
        .arg(config_path);
    if exit_after_anchors {
        command.arg("--exit-after-anchors");
    }
    command
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
/// cursor/group value and both financial logs. The older offline census still passes;
/// checkpoint preparation refuses the same row first and publishes nothing.
#[tokio::test]
async fn post_snapshot_invalid_continuation_refuses_real_binary_boot() {
    pre_start_boot_membership_case(PreStartBootCase::InvalidContinuation).await;
}

/// PASS: checkpoint restores exactly the full walk's receipts and raw obligations, decodes only
/// its suffix, and freezes the initial shared-index prefix despite later appends/consumption.
#[test]
fn paper_service_rollout_checkpoint_freezes_prefix_and_replays_suffix_exactly() {
    let (_dir, paths) = installed_fixture();
    append(
        &paths.source_log,
        activity_envelope("0xcheckpoint-first", NOW_UNIX + 1),
    );
    let opened = SourceLogBoot::open(&paths, false).unwrap().unwrap();
    let frozen_tail = opened.binding.clone();
    let mut boot = opened.boot;
    let mut sink = opened.sink;
    sink.append_durable(activity_envelope("0xcheckpoint-second", NOW_UNIX + 2))
        .unwrap();
    boot.extend(&mut sink).unwrap();
    let paper = PaperStateDb::open(&paths.fixed_main).unwrap();
    let expected = boot.obligations(&paper, &paths.paper_log).unwrap();
    publish_owner_initial(boot).unwrap();
    let artifact = std::fs::read(paths.source_log.with_extension("log.boot-checkpoint")).unwrap();
    let projection: serde_json::Value = serde_json::from_slice(&artifact[65..]).unwrap();
    assert_eq!(
        projection["tail"]["physical_tail"],
        frozen_tail.physical_tail
    );
    assert_eq!(
        projection["receipt_count"].as_u64().unwrap(),
        frozen_tail.last_sequence.unwrap().0 + 1
    );
    assert_eq!(projection["format_version"], 2);
    assert!(projection.get("receipts").is_none());
    drop(sink);
    let before = pe_event_log::scan_metrics::decoded_count(&paths.source_log).unwrap();
    let opened = SourceLogBoot::open(&paths, false).unwrap().unwrap();
    assert_eq!(
        pe_event_log::scan_metrics::decoded_count(&paths.source_log).unwrap() - before,
        1
    );
    let mut restored = opened.boot;
    let mut sink = opened.sink;
    let index = SourceReceiptIndex::replay(&paths.source_log).unwrap();
    assert_same_receipts(
        &restored.receipt_index(),
        &index,
        opened.binding.last_sequence.unwrap().0,
    );
    restored.extend(&mut sink).unwrap();
    assert_eq!(
        restored.obligations(&paper, &paths.paper_log).unwrap(),
        expected
    );
    restored.verify_handoff(&mut sink).unwrap();
}

#[test]
fn paper_service_rollout_checkpoint_v1_conversion_preserves_boot_index() {
    let (_dir, paths) = installed_fixture();
    append(
        &paths.source_log,
        activity_envelope("0xlegacy-checkpoint", NOW_UNIX + 1),
    );
    SourceLogBoot::prepare_checkpoint(&paths.fixed_main).unwrap();
    let expected = SourceReceiptIndex::replay(&paths.source_log).unwrap();
    let manifest = pe_service::source_checkpoint::checkpoint_path(&paths.source_log);
    let receipts: Vec<_> = pe_event_log::Reader::replay_with_offsets(&paths.source_log).unwrap()
        .map(|item| {
            let (offset, sequence, envelope) = item.unwrap();
            serde_json::json!({
                "receipt": AppendReceipt { sequence, this_hash: envelope.this_hash },
                "received_millis": i64::try_from(envelope.received_at.0.unix_timestamp_nanos() / 1_000_000).unwrap(),
                "byte_offset": offset,
            })
        }).collect();
    rewrite_boot_checkpoint(&manifest, |data| {
        data["format_version"] = serde_json::json!(1);
        data["receipts"] = serde_json::json!(receipts);
        data.as_object_mut().unwrap().remove("receipt_count");
        data.as_object_mut().unwrap().remove("generation");
    });
    std::fs::remove_file(checkpoint_sidecar(&paths.source_log, ".receipts")).unwrap();
    let before = pe_event_log::scan_metrics::decoded_count(&paths.source_log).unwrap();
    let first = SourceLogBoot::open(&paths, false).unwrap().unwrap();
    assert_eq!(
        pe_event_log::scan_metrics::decoded_count(&paths.source_log).unwrap(),
        before
    );
    assert_same_receipts(
        &first.boot.receipt_index(),
        &expected,
        first.binding.last_sequence.unwrap().0,
    );
    assert_eq!(checkpoint_json(&paths.source_log)["format_version"], 2);
    let converted = std::fs::read(&manifest).unwrap();
    let sidecar = std::fs::read(checkpoint_sidecar(&paths.source_log, ".receipts")).unwrap();
    drop(first);
    let second = SourceLogBoot::open(&paths, false).unwrap().unwrap();
    assert_same_receipts(
        &second.boot.receipt_index(),
        &expected,
        second.binding.last_sequence.unwrap().0,
    );
    assert_eq!(std::fs::read(manifest).unwrap(), converted);
    assert_eq!(
        std::fs::read(checkpoint_sidecar(&paths.source_log, ".receipts")).unwrap(),
        sidecar
    );
}

#[test]
fn paper_service_rollout_checkpoint_damage_incompatibility_and_prefix_drift_fall_back() {
    for fault in [
        "checksum",
        "mode",
        "version",
        "receipt",
        "receipt_count",
        "receipt_offset_missing",
        "receipt_offset_order",
        "receipt_checksum",
        "torn_receipts",
        "receipt_tail",
        "shortened",
        "corruption",
    ] {
        let (_dir, paths) = installed_fixture();
        append(
            &paths.source_log,
            activity_envelope("0xcheckpoint-fallback", NOW_UNIX + 1),
        );
        SourceLogBoot::prepare_checkpoint(&paths.fixed_main).unwrap();
        let sidecar = paths.source_log.with_extension("log.boot-checkpoint");
        let mut artifact = std::fs::read(&sidecar).unwrap();
        let mut data: serde_json::Value = serde_json::from_slice(&artifact[65..]).unwrap();
        match fault {
            "checksum" => artifact[0] ^= 1,
            "mode" => data["financial_era"] = serde_json::json!(true),
            "version" => data["scanner_version"] = serde_json::json!(99),
            "receipt" => damage_checkpoint_receipt(&paths.source_log, 0, |bytes| {
                bytes[40..48].copy_from_slice(&99999_u64.to_le_bytes());
            }),
            "receipt_count" => {
                data["receipt_count"] =
                    serde_json::json!(data["receipt_count"].as_u64().unwrap() + 1);
            }
            "receipt_offset_missing" => damage_checkpoint_receipt(&paths.source_log, 0, |bytes| {
                bytes[40..48].copy_from_slice(&u64::MAX.to_le_bytes());
            }),
            "receipt_offset_order" => damage_checkpoint_receipt(&paths.source_log, 1, |bytes| {
                bytes[40..48].copy_from_slice(&pe_event_log::HEADER_LEN.to_le_bytes());
            }),
            "receipt_checksum" => {
                let receipts = checkpoint_sidecar(&paths.source_log, ".receipts");
                let mut bytes = std::fs::read(&receipts).unwrap();
                bytes[0] ^= 1;
                std::fs::write(receipts, bytes).unwrap();
            }
            "torn_receipts" => {
                let receipts = checkpoint_sidecar(&paths.source_log, ".receipts");
                let file = std::fs::OpenOptions::new()
                    .write(true)
                    .open(receipts)
                    .unwrap();
                file.set_len(file.metadata().unwrap().len() - 1).unwrap();
            }
            "receipt_tail" => {
                let last = data["receipt_count"].as_u64().unwrap() - 1;
                damage_checkpoint_receipt(
                    &paths.source_log,
                    usize::try_from(last).unwrap(),
                    |bytes| {
                        bytes[0] ^= 1;
                    },
                );
            }
            "shortened" => {
                std::fs::OpenOptions::new()
                    .write(true)
                    .open(&paths.source_log)
                    .unwrap()
                    .set_len(recorded_source_tail(&paths))
                    .unwrap();
            }
            "corruption" => {
                let mut bytes = std::fs::read(&paths.source_log).unwrap();
                let last = bytes.len() - 1;
                bytes[last] ^= 1;
                std::fs::write(&paths.source_log, bytes).unwrap();
            }
            _ => unreachable!(),
        }
        if matches!(fault, "mode" | "version" | "receipt_count") {
            let projections = serde_json::to_vec(&data).unwrap();
            artifact = blake3::hash(&projections).to_hex().as_bytes().to_vec();
            artifact.push(b'\n');
            artifact.extend_from_slice(&projections);
        }
        std::fs::write(&sidecar, &artifact).unwrap();
        let before = pe_event_log::scan_metrics::decoded_count(&paths.source_log).unwrap();
        let opened = SourceLogBoot::open(&paths, false);
        if fault == "corruption" {
            let opened = opened.unwrap().unwrap();
            let failure = publish_owner_initial(opened.boot).unwrap_err();
            assert!(failure.message.contains("prefix mismatch"));
            assert!(
                pe_service::source_checkpoint::read_authority(&paths.source_log)
                    .unwrap()
                    .generation()
                    .unwrap()
                    > 0
            );
            drop(opened.sink);
            assert!(
                format!("{:#}", SourceLogBoot::open(&paths, false).err().unwrap()).contains("CRC")
            );
        } else {
            let opened = opened.unwrap().unwrap();
            assert_eq!(opened.binding, Scanner::verify(&paths.source_log).unwrap());
        }
        if fault.starts_with("receipt") || fault == "torn_receipts" {
            assert!(!sidecar.exists());
            assert!(!checkpoint_sidecar(&paths.source_log, ".receipts").exists());
            assert!(
                !pe_service::source_checkpoint::read_authority(&paths.source_log)
                    .unwrap()
                    .permits_checkpoint()
            );
            let expected = SourceReceiptIndex::replay(&paths.source_log).unwrap();
            let opened = SourceLogBoot::open(&paths, false).unwrap().unwrap();
            assert_same_receipts(
                &opened.boot.receipt_index(),
                &expected,
                opened.binding.last_sequence.unwrap().0,
            );
        }
        if fault != "corruption" {
            assert!(pe_event_log::scan_metrics::decoded_count(&paths.source_log).unwrap() > before);
        }
    }
}

#[test]
fn checkpoint_corrupt_receipt_quarantines_both_rebuilds_and_boots_cleanly() {
    use pe_service::source_checkpoint::{Authority, InvalidationRecord};
    use std::io::Read;

    for publisher in ["runtime", "preparation"] {
        let (_dir, paths) = installed_fixture();
        append(
            &paths.source_log,
            activity_envelope("0xcheckpoint-recovery", NOW_UNIX + 1),
        );
        let (prepared, _) = SourceLogBoot::prepare_checkpoint(&paths.fixed_main).unwrap();
        let manifest = pe_service::source_checkpoint::checkpoint_path(&paths.source_log);
        let receipts = checkpoint_sidecar(&paths.source_log, ".receipts");
        let original_records = std::fs::read(&receipts).unwrap();
        let mut corrupt_records = original_records.clone();
        corrupt_records[0] ^= 1;
        corrupt_records.extend_from_slice(&[0; 80]);
        std::fs::write(&receipts, &corrupt_records).unwrap();
        let mut quarantined_file = std::fs::File::open(&receipts).unwrap();
        let source_bytes = std::fs::read(&paths.source_log).unwrap();
        let expected = SourceReceiptIndex::replay(&paths.source_log).unwrap();
        let before = pe_event_log::scan_metrics::decoded_count(&paths.source_log).unwrap();

        let opened = SourceLogBoot::open(&paths, false).unwrap().unwrap();
        assert!(!manifest.exists());
        assert!(!receipts.exists());
        assert_eq!(
            pe_service::source_checkpoint::read_authority(&paths.source_log).unwrap(),
            Authority::Readable(InvalidationRecord {
                generation: 1,
                active: true,
                retention_fence: false
            })
        );
        assert!(pe_event_log::scan_metrics::decoded_count(&paths.source_log).unwrap() > before);
        assert_same_receipts(
            &opened.boot.receipt_index(),
            &expected,
            opened.binding.last_sequence.unwrap().0,
        );
        if publisher == "runtime" {
            publish_owner_initial(opened.boot).unwrap();
            drop(opened.sink);
        } else {
            drop(opened);
            SourceLogBoot::prepare_checkpoint(&paths.fixed_main).unwrap();
        }

        assert_eq!(std::fs::read(&receipts).unwrap(), original_records);
        let mut discarded_bytes = Vec::new();
        quarantined_file.read_to_end(&mut discarded_bytes).unwrap();
        assert_eq!(discarded_bytes, corrupt_records);
        assert_eq!(std::fs::read(&paths.source_log).unwrap(), source_bytes);
        assert_eq!(checkpoint_json(&paths.source_log)["generation"], 1);
        assert_eq!(
            pe_service::source_checkpoint::read_authority(&paths.source_log).unwrap(),
            Authority::Readable(InvalidationRecord {
                generation: 1,
                active: false,
                retention_fence: false
            })
        );
        assert_checkpoint_assisted_boot(&paths, &prepared);
        let restored = SourceLogBoot::open(&paths, false).unwrap().unwrap();
        assert_same_receipts(
            &restored.boot.receipt_index(),
            &expected,
            restored.binding.last_sequence.unwrap().0,
        );
    }
}

#[tokio::test]
async fn paper_service_rollout_checkpoint_preparation_is_early_read_only_and_ignores_torn_tail() {
    let (_dir, paths) = installed_fixture();
    let mut writer = Writer::open(&paths.source_log).unwrap();
    writer
        .append_synced(activity_envelope("0xonline-checkpoint", NOW_UNIX + 1))
        .unwrap();
    let before_db = std::fs::read(&paths.fixed_main).unwrap();
    let complete_size = file_len(&paths.source_log);
    let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_pe-service"));
    command
        .env_clear()
        .env("PE_BIND", "invalid")
        .arg("--prepare-source-checkpoint")
        .arg("--paper-state")
        .arg(&paths.fixed_main);
    let output = support::bounded_command_output(command).await;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert_eq!(stdout.lines().count(), 1);
    assert!(stdout.starts_with("source checkpoint published offset="));
    let fields: std::collections::HashMap<_, _> = stdout
        .split_whitespace()
        .filter_map(|part| part.split_once('='))
        .collect();
    assert_eq!(fields["offset"].parse::<u64>().unwrap(), complete_size);
    let tail = writer.verified_tail().unwrap();
    assert_eq!(
        fields["sequence"],
        tail.last_sequence.unwrap().0.to_string()
    );
    assert_eq!(fields["hash"], tail.last_hash.to_hex().as_str());
    assert_eq!(
        fields["prefix_blake3"],
        Scanner::hash_prefix(&paths.source_log, complete_size)
            .unwrap()
            .finalize()
            .to_hex()
            .as_str()
    );
    assert_eq!(fields["validated"], "0");
    assert!(
        fields["published_unix_ms"].parse::<u64>().unwrap()
            >= fields["capture_unix_ms"].parse::<u64>().unwrap()
    );
    assert_eq!(std::fs::read(&paths.fixed_main).unwrap(), before_db);
    assert_eq!(file_len(&paths.source_log), complete_size);
    writer
        .append_synced(activity_envelope("0xonline-suffix", NOW_UNIX + 2))
        .unwrap();
    drop(writer);
    let complete_tail = Scanner::verify(&paths.source_log).unwrap();
    std::fs::OpenOptions::new()
        .append(true)
        .open(&paths.source_log)
        .unwrap()
        .write_all(&[1, 2])
        .unwrap();
    let torn_size = file_len(&paths.source_log);
    let prepared = SourceLogBoot::prepare_checkpoint(&paths.fixed_main).unwrap();
    assert_eq!(prepared.0.tail, complete_tail);
    assert_eq!(prepared.1, 0);
    assert_eq!(file_len(&paths.source_log), torn_size);
    assert_eq!(std::fs::read(&paths.fixed_main).unwrap(), before_db);
    let opened = SourceLogBoot::open(&paths, false).unwrap().unwrap();
    assert_eq!(opened.binding, complete_tail);
    assert_eq!(file_len(&paths.source_log), complete_tail.physical_tail);
}

/// PASS: the identical unmatched-Prepared/open-continuation snapshot recovers identically with
/// decoded full-walk metadata and restored checkpoint metadata, including the resumed terminal.
#[tokio::test]
async fn paper_service_rollout_checkpoint_matches_full_walk_financial_and_pending_recovery() {
    post_start_boot_bankroll_case_with_checkpoint_parity(
        PostStartBootCase::AuthorityAheadLocalUntouched,
        true,
    )
    .await;
}

/// The bucket owner emits complete-read wires; version 6 is the historical edited wire.
/// The synthetic frame capture uses production admission hashing and authentication, followed
/// by the same financial continuation owner as a production frame.
fn install_mixed_current_open_continuations(
    paper: &Arc<PaperStateDb>,
    source_log: &Path,
    paper_prefix: AppendReceipt,
) {
    use pe_service::bucket_commit::{
        BucketCommitEngine, FrozenDecisionBasis, PaperFreshnessPolicy,
    };
    use pe_service::frame_admission::{
        FeedHistoryFrontier, FeedLatchBasis, FrameAdmissionInputs, FrameDecisionProof,
    };
    for (version, wallet) in [
        (6, WalletAddress([0xcc; 20])),
        (7, WalletAddress([0xcd; 20])),
        (7, WalletAddress([0xcf; 20])),
    ] {
        support::install_verified_empty_anchor(paper, wallet, 0);
        paper
            .record_reconciled_history_status(&pe_paper_state::WalletHistoryStatusRecord {
                wallet,
                complete: true,
                proof_json: "{}".to_owned(),
                updated_at_unix: NOW_UNIX,
            })
            .unwrap();
        let payload = serde_json::to_vec(&serde_json::json!([{
            "proxyWallet": wallet, "timestamp": NOW_UNIX + 25_000, "conditionId": format!("0x{:064x}", wallet.0[0]), "type": "TRADE", "size": "2.5", "usdcSize": "1.25",
            "transactionHash": format!("0x{:064x}", wallet.0[0]), "price": "0.5", "asset": wallet.0[0].to_string(), "side": "BUY", "outcomeIndex": 0, "outcome": "Yes", "isCombo": false
        }])).unwrap();
        let mut writer = Writer::open(source_log).unwrap();
        let (read, receipt) = support::append_committed_read_v2(
            &mut writer,
            wallet,
            &payload,
            NOW_UNIX + 25_001,
            NOW_UNIX + 25_002,
        );
        drop(writer);
        let mut context = support::read_context(&read, receipt, NOW_UNIX + 25_003);
        let mut configuration = context.applied_configuration.clone();
        configuration.sizing_mode = pe_strategy_winner_follow::SizingMode::Dollar { usd: dec!(2) };
        configuration.sizing_dollar_usd = dec!(2);
        configuration.slippage_rate = Decimal::ZERO;
        configuration.per_trade_cap = pe_strategy_winner_follow::PerTradeCap::Unlimited;
        configuration.min_resolution_horizon_secs = 0;
        configuration.max_resolution_horizon_secs = 0;
        if wallet == WalletAddress([0xcf; 20]) {
            configuration.sizing_mode =
                pe_strategy_winner_follow::SizingMode::Contract { contracts: 1000 };
            configuration.sizing_contracts = 1000;
        }
        context.applied_configuration = configuration;
        let mut engine = BucketCommitEngine::load(
            paper.clone(),
            pe_service::paper_recovery::build_leader_ledger(paper).unwrap(),
        )
        .unwrap();
        let result = engine
            .commit_with_freshness_policy(
                read.aggregates,
                &context,
                FrozenDecisionBasis {
                    win_rate_p: pe_core_types::Probability::new(dec!(0.7)).unwrap(),
                    bankroll: dec!(10),
                },
                Some(PaperFreshnessPolicy {
                    activity_ws_enabled: true,
                    copy_latency_budget_secs: 120,
                }),
            )
            .unwrap();
        if version == 6 {
            let row = paper
                .decision_pending_for(&result.pending[0])
                .unwrap()
                .unwrap();
            let mut wire: serde_json::Value =
                serde_json::from_str(&row.frozen_inputs_json).unwrap();
            wire["version"] = serde_json::json!(6);
            wire.as_object_mut().unwrap().remove("source_authority");
            let connection = Connection::open(source_log.with_file_name("paper_state.db")).unwrap();
            connection
                .execute(
                    "UPDATE decision_pending SET frozen_inputs_json=?1 WHERE source_trade_id=?2",
                    rusqlite::params![wire.to_string(), row.source_trade_id.0],
                )
                .unwrap();
        }
    }
    let wallet = WalletAddress([0xce; 20]);
    support::install_verified_empty_anchor(paper, wallet, 0);
    paper
        .record_reconciled_history_status(&pe_paper_state::WalletHistoryStatusRecord {
            wallet,
            complete: true,
            proof_json: "{}".to_owned(),
            updated_at_unix: NOW_UNIX,
        })
        .unwrap();
    let at = OffsetDateTime::from_unix_timestamp(NOW_UNIX + 25_002).unwrap();
    let mut writer = Writer::open(source_log).unwrap();
    let (frontier_read, commitment) = support::append_committed_read_v2(
        &mut writer,
        wallet,
        b"[]",
        NOW_UNIX + 24_999,
        NOW_UNIX + 25_001,
    );
    let payload = serde_json::to_vec(&serde_json::json!({"proxyWallet": wallet, "timestamp": NOW_UNIX + 25_000, "conditionId": "0x0000000000000000000000000000000000000000000000000000000000000008", "type": "TRADE", "size": "2.5", "usdcSize": "1.25", "transactionHash": "0x0000000000000000000000000000000000000000000000000000000000000008", "price": "0.5", "asset": "206", "side": "BUY", "outcomeIndex": 0, "outcome": "Yes", "isCombo": false})).unwrap();
    let observation =
        pe_source_polymarket_public::parse_activity_trade_observation(&payload).unwrap();
    let frame = writer
        .append_synced(EnvelopeIn {
            source_id: SourceId(ACTIVITY_WS_SOURCE_ID.to_owned()),
            schema_version: 2,
            parser_version: 2,
            observed_at: observation.source_time.clone(),
            received_at: ReceivedAt(at),
            content_type: ContentType::Json,
            payload,
        })
        .unwrap();
    let ledger = pe_service::paper_recovery::build_leader_ledger(paper).unwrap();
    let inputs = FrameAdmissionInputs {
        version: 1,
        frame_receipt: frame,
        admitted_at: at,
        received_at: at,
        source_time: observation.source_time.0,
        ledger_capture: pe_service::position_seeder::ledger_capture(&ledger, paper, wallet)
            .unwrap(),
        ledger_group_boundary: None,
        anchor_balances: Vec::new(),
        ledger_groups: Vec::new(),
        market_consumed: false,
        earlier_frames: Vec::new(),
        copy_eligible: true,
        history_complete: true,
        fenced: false,
        coverage: paper.wallet_coverage(&wallet).unwrap(),
        frontier: FeedHistoryFrontier {
            version: 1,
            wallet,
            fixed_end: NOW_UNIX + 24_999,
            commitment,
            page_occurrences: vec![frontier_read.page],
            pages: serde_json::from_str::<serde_json::Value>(&frontier_read.decision_inputs_json)
                .unwrap()["pages"]
                .as_array()
                .unwrap()
                .iter()
                .map(|page| serde_json::from_value(page.clone()).unwrap())
                .collect(),
        },
        poll_round_stale_secs: 90,
        latch: FeedLatchBasis::default(),
        paper_prefix: Some(paper_prefix),
        identity: None,
    };
    let revision = pe_service::frame_admission::FrameAdmissionArtifact::from_inputs(&inputs)
        .unwrap()
        .capture_digest;
    let admission = writer
        .append_synced(EnvelopeIn {
            source_id: SourceId(pe_service::frame_admission::FRAME_ADMISSION_SOURCE_ID.to_owned()),
            schema_version: 1,
            parser_version: 1,
            observed_at: SourceTimestamp(at),
            received_at: ReceivedAt(at),
            content_type: ContentType::Json,
            payload: serde_json::to_vec(
                &pe_service::frame_admission::FrameAdmissionArtifact::from_inputs(&inputs).unwrap(),
            )
            .unwrap(),
        })
        .unwrap();
    let rest_row = paper
        .decision_pending_history()
        .unwrap()
        .into_iter()
        .find(|row| row.wallet == WalletAddress([0xcd; 20]))
        .unwrap();
    let mut wire: serde_json::Value = serde_json::from_str(&rest_row.frozen_inputs_json).unwrap();
    let id = observation.group_id.key().clone();
    wire["source_authority"] = serde_json::json!("activity_frame");
    wire["source_trade_id"] = serde_json::json!(id);
    wire["semantic_revision"] = serde_json::json!(revision);
    wire["wallet"] = serde_json::to_value(wallet).unwrap();
    wire["transaction_hash"] =
        serde_json::json!("0x0000000000000000000000000000000000000000000000000000000000000008");
    wire["market_id"] =
        serde_json::json!("0x0000000000000000000000000000000000000000000000000000000000000008");
    wire["provenance"] = serde_json::json!("activity_ws");
    wire["decision_inputs"] = serde_json::to_value(FrameDecisionProof {
        admission_receipt: admission,
        inputs,
    })
    .unwrap();
    wire["observed_source_receipt"] = serde_json::to_value(frame).unwrap();
    wire["page_occurrences"] = serde_json::json!([]);
    wire.as_object_mut().unwrap().remove("read_commitment");
    paper
        .commit_activity_frame(&pe_paper_state::ActivityFrameCommit {
            gate: pe_paper_state::EntryGateResultRecord {
                source_trade_id: id.clone(),
                wallet,
                market_id: MarketId(VenueMarketId(
                    "0x0000000000000000000000000000000000000000000000000000000000000008".to_owned(),
                )),
                source_epoch: NOW_UNIX + 25_000,
                result: "admitted".to_owned(),
                history_consumed: true,
            },
            history: pe_paper_state::MarketHistoryRecord {
                wallet,
                market_id: MarketId(VenueMarketId(
                    "0x0000000000000000000000000000000000000000000000000000000000000008".to_owned(),
                )),
                first_epoch: NOW_UNIX + 25_000,
                source_trade_id: id.clone(),
            },
            pending: pe_paper_state::DecisionPendingRecord {
                source_trade_id: id,
                semantic_revision: revision,
                wallet,
                source_epoch: NOW_UNIX + 25_000,
                frozen_inputs_json: wire.to_string(),
                updated_at_unix: NOW_UNIX + 25_002,
            },
        })
        .unwrap();
}

fn mixed_continuation_market_evidence(
    source_path: &Path,
    paper: &PaperStateDb,
) -> (
    Arc<pe_service::orchestrator::ScenarioHooks>,
    support::Prices,
    std::collections::HashMap<String, pe_service::clob_book::OrderBook>,
) {
    use pe_core_types::{PolymarketConditionId, ShareAmount};
    use pe_execution_core::{AdmissionReceipts, LiveAdmissionArtifact};
    use pe_resolver_card::{
        VENUE_SETTLEMENT_SCHEMA_VERSION, VenueResolutionStatus, VenueSettlementRecord,
    };
    use pe_source_polymarket_public::validate_paper_market;
    use pe_venue_polymarket::parse_compact_market;
    let clock = NOW_UNIX + 25_010;
    let hooks = support::continuation_hooks(clock);
    hooks.age_clock.lock().unwrap().clear();
    hooks.age_clock.lock().unwrap().extend(std::iter::repeat_n(
        OffsetDateTime::from_unix_timestamp(clock).unwrap(),
        96,
    ));
    hooks.admission_artifacts.lock().unwrap().clear();
    let prices = support::Prices {
        gate: Arc::new(support::PriceGate::default()),
        markets: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
    };
    for position in paper.open_positions().unwrap() {
        let market = position.market_id.to_string();
        prices.markets.lock().unwrap().insert(market.clone(), serde_json::json!({
            "conditionId": market, "outcomePrices": "[\"0.50\",\"0.50\"]", "active": true, "closed": false
        }));
    }
    let mut books = std::collections::HashMap::new();
    let mut writer = Writer::open(source_path).unwrap();
    for row in paper.open_decision_pending().unwrap() {
        let continuation =
            pe_service::bucket_commit::DecisionContinuationV3::from_durable(&row).unwrap();
        let condition = continuation.facts.market_id.to_string();
        let token = row.wallet.0[0].to_string();
        let mut gamma: serde_json::Value =
            serde_json::from_slice(include_bytes!("fixtures/golden_stream_v1/gamma_long.json"))
                .unwrap();
        gamma[0]["conditionId"] = serde_json::json!(condition);
        gamma[0]["clobTokenIds"] =
            serde_json::json!([token, (u16::from(row.wallet.0[0]) + 1000).to_string()])
                .to_string()
                .into();
        gamma[0]["orderMinSize"] = serde_json::json!("1");
        gamma[0]["outcomePrices"] = serde_json::json!("[\"0.50\",\"0.50\"]");
        let mut long: serde_json::Value =
            serde_json::from_slice(include_bytes!("fixtures/golden_stream_v1/clob_long.json"))
                .unwrap();
        long["condition_id"] = serde_json::json!(condition);
        long["minimum_order_size"] = serde_json::json!("1");
        long["tokens"][0]["token_id"] = serde_json::json!(token);
        long["tokens"][1]["token_id"] =
            serde_json::json!((u16::from(row.wallet.0[0]) + 1000).to_string());
        let mut compact: serde_json::Value = serde_json::from_slice(include_bytes!(
            "fixtures/golden_stream_v1/clob_compact.json"
        ))
        .unwrap();
        compact["c"] = serde_json::json!(condition);
        compact["mos"] = serde_json::json!("1");
        compact["fd"]["r"] = serde_json::json!(0);
        compact["t"][0]["t"] = serde_json::json!(token);
        compact["t"][1]["t"] = serde_json::json!((u16::from(row.wallet.0[0]) + 1000).to_string());
        let gamma_bytes = serde_json::to_vec(&gamma).unwrap();
        let long_bytes = serde_json::to_vec(&long).unwrap();
        let compact_bytes = serde_json::to_vec(&compact).unwrap();
        let receipts = AdmissionReceipts {
            gamma: writer
                .append_synced(envelope(
                    "polymarket.gamma.markets",
                    1,
                    1,
                    &gamma_bytes,
                    clock,
                ))
                .unwrap(),
            clob_long: writer
                .append_synced(envelope(
                    "polymarket.clob.markets",
                    1,
                    1,
                    &long_bytes,
                    clock,
                ))
                .unwrap(),
            clob_compact: writer
                .append_synced(envelope(
                    "polymarket.clob.compact-market",
                    1,
                    1,
                    &compact_bytes,
                    clock,
                ))
                .unwrap(),
        };
        let condition_id = PolymarketConditionId(condition.clone());
        let market =
            validate_paper_market(&gamma_bytes, &long_bytes, &condition_id, clock, 60).unwrap();
        assert_eq!(
            market.minimum_order_size,
            ShareAmount::from_whole(1).unwrap()
        );
        let fee_schedule = parse_compact_market(
            &compact_bytes,
            &condition_id,
            &market.ordered_outcome_token_ids,
        )
        .unwrap()
        .fee_schedule;
        hooks
            .admission_artifacts
            .lock()
            .unwrap()
            .push_back(LiveAdmissionArtifact {
                market,
                fee_schedule,
                receipts,
                settlement: VenueSettlementRecord {
                    schema_version: VENUE_SETTLEMENT_SCHEMA_VERSION,
                    condition_id,
                    status: VenueResolutionStatus::Unresolved,
                    raw_evidence_hash: blake3::hash(&long_bytes).to_hex().to_string(),
                    source_timestamp_unix: None,
                    observed_at_unix: clock,
                    parser_version: 1,
                    freshness_window_secs: 60,
                },
            });
        prices
            .markets
            .lock()
            .unwrap()
            .insert(condition.clone(), gamma[0].clone());
        let mut book: serde_json::Value =
            serde_json::from_slice(include_bytes!("fixtures/golden_stream_v1/book.json")).unwrap();
        book["market"] = serde_json::json!(condition);
        book["asset_id"] = serde_json::json!(token);
        book["min_order_size"] = serde_json::json!("1");
        book["asks"] = serde_json::json!([{ "price": "0.50", "size": if continuation.is_activity_frame() { "1.5" } else { "100" } }]);
        let bytes = serde_json::to_vec(&book).unwrap();
        let receipt = writer
            .append_synced(envelope("polymarket.clob.book", 1, 1, &bytes, clock))
            .unwrap();
        let mut book = pe_service::clob_book::OrderBook::from_book_json(&bytes).unwrap();
        book.source_receipt = Some(receipt);
        book.fetched_at_ms = u64::try_from(clock * 1000).unwrap();
        books.insert(token, book);
    }
    (hooks, prices, books)
}

fn rewrite_boot_checkpoint(path: &Path, edit: impl FnOnce(&mut serde_json::Value)) {
    let bytes = std::fs::read(path).unwrap();
    let mut body: serde_json::Value = serde_json::from_slice(&bytes[65..]).unwrap();
    edit(&mut body);
    let encoded = serde_json::to_vec(&body).unwrap();
    let mut artifact = blake3::hash(&encoded).to_hex().as_bytes().to_vec();
    artifact.push(b'\n');
    artifact.extend_from_slice(&encoded);
    std::fs::write(path, &artifact).unwrap();
}

fn checkpoint_record_path(source_log: &Path) -> PathBuf {
    let mut name = pe_service::source_checkpoint::checkpoint_path(source_log).into_os_string();
    name.push(".invalidation");
    name.into()
}

fn write_checkpoint_record(source_log: &Path, generation: u64, active: bool) {
    std::fs::write(
        checkpoint_record_path(source_log),
        serde_json::to_vec(&pe_service::source_checkpoint::InvalidationRecord {
            generation,
            active,
            retention_fence: false,
        })
        .unwrap(),
    )
    .unwrap();
}

fn assert_checkpoint_assisted_boot(
    paths: &PaperMigrationPaths,
    receipt: &pe_service::source_checkpoint::PublicationReceipt,
) {
    let before = pe_event_log::scan_metrics::decoded_count(&paths.source_log).unwrap();
    let opened = SourceLogBoot::open(paths, false).unwrap().unwrap();
    assert_eq!(opened.binding, receipt.tail);
    assert_eq!(
        pe_event_log::scan_metrics::decoded_count(&paths.source_log).unwrap(),
        before
    );
    let artifact = std::fs::read(pe_service::source_checkpoint::checkpoint_path(
        &paths.source_log,
    ))
    .unwrap();
    assert_eq!(
        &artifact[..64],
        blake3::hash(&artifact[65..]).to_hex().as_bytes()
    );
    let body: serde_json::Value = serde_json::from_slice(&artifact[65..]).unwrap();
    assert_eq!(body["prefix_blake3"], receipt.prefix_blake3);
}

/// PASS: wrong raw-prefix digests at unchanged tails are repaired under fresh authority; active
/// records force full verification. Changed artifacts or generations restart preparation instead
/// of invalidating the newly installed checkpoint. A post-checkpoint/pre-clearance image rebuilds.
#[test]
fn wrong_prefix_checkpoint_repaired_by_preparation() {
    use pe_service::source_checkpoint::{
        Authority, InvalidationRecord, PreparationHooks, read_authority,
    };
    use std::sync::atomic::{AtomicBool, Ordering};
    for case in [
        "inactive",
        "active",
        "interrupted_clearance",
        "artifact_changed",
        "generation_changed",
        "hash_io_error",
    ] {
        let (_dir, paths) = installed_fixture();
        append(
            &paths.source_log,
            activity_envelope("0xrepair-prefix", NOW_UNIX + 1),
        );
        let (original, _) = SourceLogBoot::prepare_checkpoint(&paths.fixed_main).unwrap();
        let sidecar = pe_service::source_checkpoint::checkpoint_path(&paths.source_log);
        let good_bytes = std::fs::read(&sidecar).unwrap();
        rewrite_boot_checkpoint(&sidecar, |body| {
            body["prefix_blake3"] = serde_json::json!("ff".repeat(32))
        });
        write_checkpoint_record(
            &paths.source_log,
            4,
            matches!(case, "active" | "interrupted_clearance"),
        );
        if case == "interrupted_clearance" {
            // The durable image after checkpoint installation and before active-record clearance.
            std::fs::write(&sidecar, &good_bytes).unwrap();
        }
        let checked = Arc::new(AtomicBool::new(false));
        let changed = checked.clone();
        let scenario_paths = paths.clone();
        let bound_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let captures = bound_calls.clone();
        let hash_paths = paths.clone();
        let original_offset = original.tail.physical_tail;
        let hooks = PreparationHooks {
            before_cached_hash: Some(Arc::new(move || {
                if case == "hash_io_error" {
                    // The loaded cache passed the length check. Shorten the file only after
                    // load so hashing reports UnexpectedEof; the remaining activation is valid.
                    std::fs::OpenOptions::new()
                        .write(true)
                        .open(&hash_paths.source_log)?
                        .set_len(recorded_source_tail(&hash_paths))?;
                }
                Ok(())
            })),
            after_bound: Some(Arc::new(move || {
                captures.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })),
            after_cached_hash: Some(Arc::new(move || {
                if !changed.swap(true, Ordering::SeqCst) {
                    if case == "artifact_changed" {
                        // A verified later-tail publisher can legitimately replace the wrong
                        // digest without changing authority; the paused preparation must restart.
                        append(
                            &scenario_paths.source_log,
                            activity_envelope("0xnew-artifact", NOW_UNIX + 2),
                        );
                        rewrite_boot_checkpoint(
                            &pe_service::source_checkpoint::checkpoint_path(
                                &scenario_paths.source_log,
                            ),
                            |body| {
                                body["prefix_blake3"] = serde_json::json!(
                                    Scanner::hash_prefix(
                                        &scenario_paths.source_log,
                                        original_offset
                                    )
                                    .unwrap()
                                    .finalize()
                                    .to_hex()
                                    .to_string()
                                );
                            },
                        );
                        SourceLogBoot::prepare_checkpoint(&scenario_paths.fixed_main).unwrap();
                    } else if case == "generation_changed" {
                        assert_eq!(
                            pe_service::source_checkpoint::invalidate(&scenario_paths.source_log)
                                .unwrap(),
                            5
                        );
                        SourceLogBoot::prepare_checkpoint(&scenario_paths.fixed_main).unwrap();
                    }
                }
                Ok(())
            })),
            ..PreparationHooks::default()
        };
        let (repaired, validated) =
            SourceLogBoot::prepare_checkpoint_with_hooks(&paths.fixed_main, &hooks).unwrap();
        assert_eq!(validated, 0);
        if matches!(case, "artifact_changed" | "hash_io_error") {
            if case == "artifact_changed" {
                assert!(repaired.tail.physical_tail > original.tail.physical_tail);
            } else {
                assert!(repaired.tail.physical_tail < original.tail.physical_tail);
            }
            assert_eq!(repaired.tail, Scanner::verify(&paths.source_log).unwrap());
            assert_eq!(
                repaired.prefix_blake3,
                Scanner::hash_prefix(&paths.source_log, repaired.tail.physical_tail)
                    .unwrap()
                    .finalize()
                    .to_hex()
                    .to_string()
            );
        } else {
            assert_eq!(repaired.tail, original.tail);
            assert_eq!(repaired.prefix_blake3, original.prefix_blake3);
        }
        assert_eq!(
            bound_calls.load(Ordering::SeqCst),
            if matches!(case, "artifact_changed" | "generation_changed") {
                2
            } else {
                1
            }
        );
        let expected_generation = if matches!(case, "inactive" | "generation_changed") {
            5
        } else {
            4
        };
        assert_eq!(
            read_authority(&paths.source_log).unwrap(),
            Authority::Readable(InvalidationRecord {
                generation: expected_generation,
                active: false,
                retention_fence: false
            }),
            "{case}"
        );
        assert_checkpoint_assisted_boot(&paths, &repaired);
    }
}

/// PASS: the same loader checks governing boot also allow publication to replace an unusable
/// artifact, including a checksum-valid tail beyond physical EOF; the next boot uses the repair.
#[test]
fn unusable_checkpoint_replaced_by_verified_candidate() {
    for fault in [
        "absent",
        "checksum",
        "decode",
        "mode",
        "format",
        "scanner",
        "reducer",
        "receipt",
        "long_tail",
    ] {
        let (_dir, paths) = installed_fixture();
        append(
            &paths.source_log,
            activity_envelope("0xreplace-unusable", NOW_UNIX + 1),
        );
        let (receipt, _) = SourceLogBoot::prepare_checkpoint(&paths.fixed_main).unwrap();
        let sidecar = pe_service::source_checkpoint::checkpoint_path(&paths.source_log);
        match fault {
            "absent" => std::fs::remove_file(&sidecar).unwrap(),
            "checksum" => {
                let mut bytes = std::fs::read(&sidecar).unwrap();
                bytes[0] ^= 1;
                std::fs::write(&sidecar, bytes).unwrap();
            }
            "decode" => std::fs::write(&sidecar, b"damaged").unwrap(),
            _ => rewrite_boot_checkpoint(&sidecar, |data| match fault {
                "mode" => data["financial_era"] = serde_json::json!(true),
                "format" => data["format_version"] = serde_json::json!(99),
                "scanner" => data["scanner_version"] = serde_json::json!(99),
                "reducer" => data["reducer_version"] = serde_json::json!(1),
                "receipt" => data["receipt_count"] = serde_json::json!(0),
                "long_tail" => {
                    data["tail"]["physical_tail"] =
                        serde_json::json!(file_len(&paths.source_log) + 1)
                }
                _ => unreachable!(),
            }),
        }
        let before = pe_event_log::scan_metrics::decoded_count(&paths.source_log).unwrap();
        let opened = SourceLogBoot::open(&paths, false).unwrap().unwrap();
        assert_eq!(opened.binding, receipt.tail);
        assert!(
            pe_event_log::scan_metrics::decoded_count(&paths.source_log).unwrap() > before,
            "{fault}"
        );
        publish_owner_initial(opened.boot).unwrap();
        drop(opened.sink);
        assert_checkpoint_assisted_boot(&paths, &receipt);
    }
}

/// PASS: a real preparation command paused after verification cannot publish over an interrupted
/// invalidation. Its refusal preserves the active record, and the next boot full-walks.
#[tokio::test]
async fn checkpoint_cli_publisher_refused_after_interrupted_invalidation() {
    use pe_service::source_checkpoint::{
        Authority, InvalidationError, InvalidationHooks, InvalidationRecord,
    };
    use std::sync::atomic::Ordering;
    for inactive_record in [false, true] {
        let (dir, paths) = installed_fixture();
        append(
            &paths.source_log,
            activity_envelope("0xcli-race", NOW_UNIX + 1),
        );
        SourceLogBoot::prepare_checkpoint(&paths.fixed_main).unwrap();
        if inactive_record {
            write_checkpoint_record(&paths.source_log, 0, false);
        }
        let pause = dir.path().join("preparation-pause");
        let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_pe-service"));
        command
            .env_clear()
            .env("PE_SCENARIO_CHECKPOINT_PAUSE_AFTER_WALK", &pause)
            .arg("--prepare-source-checkpoint")
            .arg("--paper-state")
            .arg(&paths.fixed_main);
        let publisher = tokio::spawn(support::bounded_command_output(command));
        tokio::time::timeout(std::time::Duration::from_secs(15), async {
            while !pause.with_extension("ready").exists() {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let hooks = InvalidationHooks::default();
        hooks.fail_quarantine_sync.store(true, Ordering::SeqCst);
        assert!(matches!(
            pe_service::source_checkpoint::invalidate_with_hooks(&paths.source_log, &hooks),
            Err(InvalidationError::QuarantineFailed(_))
        ));
        std::fs::write(pause.with_extension("resume"), b"resume").unwrap();
        let output = publisher.await.unwrap();
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains("GenerationChanged"));
        assert!(pe_service::source_checkpoint::checkpoint_path(&paths.source_log).exists());
        assert_eq!(
            pe_service::source_checkpoint::read_authority(&paths.source_log).unwrap(),
            Authority::Readable(InvalidationRecord {
                generation: 1,
                active: true,
                retention_fence: false
            })
        );
        let before = pe_event_log::scan_metrics::decoded_count(&paths.source_log).unwrap();
        let opened = SourceLogBoot::open(&paths, false).unwrap().unwrap();
        assert!(pe_event_log::scan_metrics::decoded_count(&paths.source_log).unwrap() > before);
        publish_owner_initial(opened.boot).unwrap();
        assert_eq!(
            pe_service::source_checkpoint::read_authority(&paths.source_log).unwrap(),
            Authority::Readable(InvalidationRecord {
                generation: 1,
                active: false,
                retention_fence: false
            })
        );
    }
}

/// PASS: quiesced recovery installs an active record before removing checkpoint state even in
/// the durable image preceding a failed directory sync; full verification and publication restore later boots.
#[tokio::test]
async fn checkpoint_failed_quarantine_sync_durable_image_recovers_with_full_walk() {
    use pe_service::source_checkpoint::{
        Authority, InvalidationError, InvalidationHooks, InvalidationRecord,
    };
    use std::sync::atomic::Ordering;
    let (_dir, paths) = installed_fixture();
    append(
        &paths.source_log,
        activity_envelope("0xquarantine-sync", NOW_UNIX + 1),
    );
    let (receipt, _) = SourceLogBoot::prepare_checkpoint(&paths.fixed_main).unwrap();
    let sidecar = pe_service::source_checkpoint::checkpoint_path(&paths.source_log);
    let durable_checkpoint = std::fs::read(&sidecar).unwrap();
    let receipts = checkpoint_sidecar(&paths.source_log, ".receipts");
    let durable_receipts = std::fs::read(&receipts).unwrap();
    write_checkpoint_record(&paths.source_log, 3, false);
    let durable_record = std::fs::read(checkpoint_record_path(&paths.source_log)).unwrap();
    let hooks = InvalidationHooks::default();
    hooks.fail_quarantine_sync.store(true, Ordering::SeqCst);
    assert!(matches!(
        pe_service::source_checkpoint::invalidate_with_hooks(&paths.source_log, &hooks),
        Err(InvalidationError::QuarantineFailed(_))
    ));
    // Model loss of the unsynced rename and removal: the preceding files all survive.
    std::fs::write(&sidecar, &durable_checkpoint).unwrap();
    std::fs::write(&receipts, &durable_receipts).unwrap();
    std::fs::write(checkpoint_record_path(&paths.source_log), &durable_record).unwrap();
    let before_db = std::fs::read(&paths.fixed_main).unwrap();
    let before_log = std::fs::read(&paths.source_log).unwrap();
    let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_pe-service"));
    command
        .env_clear()
        .env("PE_BIND", "invalid")
        .arg("--recover-source-checkpoint")
        .arg("--paper-state")
        .arg(&paths.fixed_main);
    let output = support::bounded_command_output(command).await;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.starts_with("source checkpoint recovery checkpoint="));
    assert_eq!(stdout.matches("removed=true").count(), 2);
    assert!(!sidecar.exists());
    assert!(!checkpoint_sidecar(&paths.source_log, ".receipts").exists());
    assert!(checkpoint_record_path(&paths.source_log).exists());
    assert_eq!(std::fs::read(&paths.fixed_main).unwrap(), before_db);
    assert_eq!(std::fs::read(&paths.source_log).unwrap(), before_log);
    assert_eq!(
        pe_service::source_checkpoint::read_authority(&paths.source_log).unwrap(),
        Authority::Readable(InvalidationRecord {
            generation: 4,
            active: true,
            retention_fence: false
        })
    );
    let before = pe_event_log::scan_metrics::decoded_count(&paths.source_log).unwrap();
    let opened = SourceLogBoot::open(&paths, false).unwrap().unwrap();
    assert!(pe_event_log::scan_metrics::decoded_count(&paths.source_log).unwrap() > before);
    publish_owner_initial(opened.boot).unwrap();
    drop(opened.sink);
    assert_checkpoint_assisted_boot(&paths, &receipt);
}

/// PASS: preparation and open-continuation validation ignore the stored frontier collection.
/// Runtime admission waits for a freshly authenticated read.
#[test]
fn checkpoint_preparation_does_not_restore_stored_frontiers() {
    use pe_service::frame_admission::{FeedHistoryFrontier, FrontierCollection};
    use pe_service::source_checkpoint::PreparationHooks;
    let (_dir, paths) = installed_fixture();
    let bound = Scanner::verify(&paths.source_log).unwrap();
    let hook_paths = paths.clone();
    let hooks = PreparationHooks {
        after_bound: Some(Arc::new(move || {
            let wallet = WalletAddress::from_hex(WALLET).unwrap();
            let mut writer = Writer::open(&hook_paths.source_log).unwrap();
            let (read, commitment) = support::append_committed_read_v2(
                &mut writer,
                wallet,
                b"[]",
                NOW_UNIX + 10,
                NOW_UNIX + 11,
            );
            drop(writer);
            let data: serde_json::Value = serde_json::from_str(&read.decision_inputs_json).unwrap();
            let collection = FrontierCollection {
                version: 1,
                frontiers: vec![FeedHistoryFrontier {
                    version: 1,
                    wallet,
                    fixed_end: NOW_UNIX + 10,
                    commitment,
                    page_occurrences: vec![read.page],
                    pages: serde_json::from_value(data["pages"].clone()).unwrap(),
                }],
            };
            PaperStateDb::open(&hook_paths.fixed_main)
                .unwrap()
                .publish_feed_history_frontiers(&serde_json::to_value(collection).unwrap())
                .unwrap();
            Ok(())
        })),
        ..PreparationHooks::default()
    };
    let (receipt, count) =
        SourceLogBoot::prepare_checkpoint_with_hooks(&paths.fixed_main, &hooks).unwrap();
    assert_eq!(receipt.tail, bound);
    assert_eq!(count, 0);
    let opened = SourceLogBoot::open(&paths, false).unwrap().unwrap();
    assert!(opened.binding.physical_tail > bound.physical_tail);
    let paper = PaperStateDb::open(&paths.fixed_main).unwrap();
    assert_eq!(
        pe_service::bucket_commit::validate_open_continuations(
            &paper,
            &opened.boot.receipt_index()
        )
        .unwrap(),
        0
    );
    paper
        .publish_feed_history_frontiers(&serde_json::json!({"version": 999, "frontiers": []}))
        .unwrap();
    let (_, count) = SourceLogBoot::prepare_checkpoint(&paths.fixed_main).unwrap();
    assert_eq!(count, 0);
    assert_eq!(
        pe_service::bucket_commit::validate_open_continuations(
            &paper,
            &opened.boot.receipt_index()
        )
        .unwrap(),
        0,
    );
}

#[test]
fn checkpoint_record_read_error_forces_full_walk_without_publication() {
    let (_dir, paths) = installed_fixture();
    append(
        &paths.source_log,
        activity_envelope("0xrecord-read-error", NOW_UNIX + 1),
    );
    SourceLogBoot::prepare_checkpoint(&paths.fixed_main).unwrap();
    let sidecar = pe_service::source_checkpoint::checkpoint_path(&paths.source_log);
    let artifact = std::fs::read(&sidecar).unwrap();
    std::fs::create_dir(checkpoint_record_path(&paths.source_log)).unwrap();
    let before = pe_event_log::scan_metrics::decoded_count(&paths.source_log).unwrap();
    let opened = SourceLogBoot::open(&paths, false).unwrap().unwrap();
    assert!(pe_event_log::scan_metrics::decoded_count(&paths.source_log).unwrap() > before);
    publish_owner_initial(opened.boot).unwrap();
    assert_eq!(std::fs::read(&sidecar).unwrap(), artifact);
}

fn publish_owner_initial(boot: SourceLogBoot) -> Result<(), pe_service::supervisor::TaskFailure> {
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async move {
            let slot = pe_service::source_checkpoint::CheckpointJobSlot::default();
            let mut owner = boot.into_checkpoint_owner(slot.clone());
            let result = owner.initialize_for_scenario().await;
            drop(owner);
            slot.join().await.unwrap();
            result
        })
    })
    .join()
    .unwrap()
}

#[path = "support/rollout.rs"]
mod checkpoint_rollout;
#[path = "support/golden.rs"]
mod golden;

fn checkpoint_json(path: &Path) -> serde_json::Value {
    let bytes = std::fs::read(pe_service::source_checkpoint::checkpoint_path(path)).unwrap();
    assert_eq!(&bytes[..64], blake3::hash(&bytes[65..]).to_hex().as_bytes());
    serde_json::from_slice(&bytes[65..]).unwrap()
}

fn checkpoint_events(logs: &str, message: &str) -> Vec<serde_json::Value> {
    logs.lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .filter(|line| line["message"] == message)
        .collect()
}

async fn checkpoint_until(mut predicate: impl FnMut() -> bool) {
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        while !predicate() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

/// PASS: while deferred hashing is paused, a real decision commits; verification and publication
/// follow listening, and the published digest is that of the exact frozen bound.
#[tokio::test]
async fn checkpoint_prefix_verified_after_listening() {
    let fixture = checkpoint_rollout::CheckpointFixture::new().await;
    let (prepared, _) =
        SourceLogBoot::prepare_checkpoint(&fixture.cfg.paper_state_db_path).unwrap();
    let pause = fixture.dir.path().join("prefix-pause");
    let child = checkpoint_rollout::Child::start_checkpoint(
        &fixture.config_path,
        Path::new(env!("CARGO_BIN_EXE_pe-service")),
        &[("PE_SCENARIO_CHECKPOINT_PAUSE_BEFORE_HASH", &pause)],
    );
    checkpoint_until(|| pause.with_extension("ready").exists()).await;
    fixture.wait_log("pe-service listening").await;
    fixture.copy();
    fixture.wait_copy().await;
    assert!(!fixture.logs().contains("source checkpoint prefix verified"));
    assert_eq!(
        checkpoint_events(&fixture.logs(), "source checkpoint verification completed")[0]["prefix_verification"],
        "deferred"
    );
    std::fs::write(pause.with_extension("resume"), b"resume").unwrap();
    fixture.wait_log("source checkpoint published").await;
    let output = child.finish_checkpoint(Some("-INT")).await;
    assert!(output.status.success(), "{output:?}");
    let logs = fixture.logs();
    assert!(
        logs.find("pe-service listening").unwrap()
            < logs.find("source checkpoint prefix verified").unwrap()
    );
    assert!(
        logs.find("source checkpoint prefix verified").unwrap()
            < logs.find("source checkpoint published").unwrap()
    );
    assert!(!logs.contains("source checkpoint raw prefix verified"));
    let data = checkpoint_json(&fixture.cfg.source_event_log_path);
    assert_eq!(data["tail"]["physical_tail"], prepared.tail.physical_tail);
    assert_eq!(
        data["prefix_blake3"],
        Scanner::hash_prefix(
            &fixture.cfg.source_event_log_path,
            prepared.tail.physical_tail
        )
        .unwrap()
        .finalize()
        .to_hex()
        .as_str()
    );
    assert_eq!(fixture.paper.list_fills().unwrap().len(), 1);
}

/// PASS: mismatch after load invalidates and fails critically; corrupt frames are refused on later
/// full walks. Restoring the source allows a new-generation full walk to publish and clear active.
#[tokio::test]
async fn checkpoint_prefix_mismatch_invalidates_and_full_walks() {
    let fixture = checkpoint_rollout::CheckpointFixture::new().await;
    SourceLogBoot::prepare_checkpoint(&fixture.cfg.paper_state_db_path).unwrap();
    let original = std::fs::read(&fixture.cfg.source_event_log_path).unwrap();
    let pause = fixture.dir.path().join("prefix-pause");
    let child = checkpoint_rollout::Child::start_checkpoint(
        &fixture.config_path,
        Path::new(env!("CARGO_BIN_EXE_pe-service")),
        &[("PE_SCENARIO_CHECKPOINT_PAUSE_BEFORE_HASH", &pause)],
    );
    checkpoint_until(|| pause.with_extension("ready").exists()).await;
    corrupt_checkpoint_prefix(&fixture.cfg.source_event_log_path);
    std::fs::write(pause.with_extension("resume"), b"resume").unwrap();
    let output = child.finish_checkpoint(None).await;
    assert!(!output.status.success());
    assert_ne!(output.status.code(), Some(78));
    assert!(String::from_utf8_lossy(&output.stderr).contains("prefix mismatch"));
    assert!(
        !pe_service::source_checkpoint::checkpoint_path(&fixture.cfg.source_event_log_path)
            .exists()
    );
    let authority =
        pe_service::source_checkpoint::read_authority(&fixture.cfg.source_event_log_path).unwrap();
    assert!(!authority.permits_checkpoint());
    let generation = authority.generation().unwrap();
    fixture.clear_logs();
    let output = boot_binary(&fixture.config_path, false).await;
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("CRC"));
    assert!(!fixture.logs().contains("pe-service listening"));
    {
        use std::io::{Seek, SeekFrom};
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(&fixture.cfg.source_event_log_path)
            .unwrap();
        file.seek(SeekFrom::Start(u64::try_from(original.len() - 1).unwrap()))
            .unwrap();
        file.write_all(&original[original.len() - 1..]).unwrap();
        file.sync_all().unwrap();
    }
    fixture.clear_logs();
    let child = checkpoint_rollout::Child::start_checkpoint(
        &fixture.config_path,
        Path::new(env!("CARGO_BIN_EXE_pe-service")),
        &[],
    );
    fixture.wait_log("source checkpoint published").await;
    assert_eq!(
        checkpoint_events(&fixture.logs(), "source checkpoint verification completed")[0]["prefix_verification"],
        "full_walk"
    );
    let output = child.finish_checkpoint(Some("-INT")).await;
    assert!(output.status.success(), "{output:?}");
    // Every boot installs the old-binary fence before listening; invalidation keeps it.
    assert_eq!(
        pe_service::source_checkpoint::read_authority(&fixture.cfg.source_event_log_path).unwrap(),
        pe_service::source_checkpoint::Authority::Readable(
            pe_service::source_checkpoint::InvalidationRecord {
                generation,
                active: false,
                retention_fence: true
            }
        )
    );
}

#[tokio::test]
async fn prepared_checkpoint_receipt_matches_boot() {
    let fixture = checkpoint_rollout::CheckpointFixture::new().await;
    let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_pe-service"));
    command
        .env_clear()
        .arg("--prepare-source-checkpoint")
        .arg("--paper-state")
        .arg(&fixture.cfg.paper_state_db_path);
    let prepared = support::bounded_command_output(command).await;
    assert!(prepared.status.success(), "{prepared:?}");
    let stdout = String::from_utf8(prepared.stdout).unwrap();
    let fields = stdout
        .split_whitespace()
        .filter_map(|part| part.split_once('='))
        .collect::<std::collections::HashMap<_, _>>();
    let receipts = pe_service::source_checkpoint::receipts_path(&fixture.cfg.source_event_log_path);
    assert_eq!(fields["receipts"], receipts.to_str().unwrap());
    assert_eq!(
        std::fs::metadata(&receipts).unwrap().len(),
        checkpoint_json(&fixture.cfg.source_event_log_path)["receipt_count"]
            .as_u64()
            .unwrap()
            * 80
    );
    let child = checkpoint_rollout::Child::start_checkpoint(
        &fixture.config_path,
        Path::new(env!("CARGO_BIN_EXE_pe-service")),
        &[],
    );
    fixture.wait_log("source checkpoint published").await;
    let output = child.finish_checkpoint(Some("-INT")).await;
    assert!(output.status.success(), "{output:?}");
    let events = checkpoint_events(&fixture.logs(), "source checkpoint verification completed");
    let boot = &events[0];
    assert_eq!(boot["checkpoint_used"], true);
    assert_eq!(
        boot["checkpoint_offset"].as_u64().unwrap().to_string(),
        fields["offset"]
    );
    assert_eq!(
        boot["checkpoint_sequence"].as_u64().unwrap().to_string(),
        fields["sequence"]
    );
    assert_eq!(boot["checkpoint_hash"], fields["hash"]);
    assert_eq!(boot["prefix_blake3"], fields["prefix_blake3"]);
    assert!(
        fields["published_unix_ms"].parse::<u64>().unwrap()
            >= fields["capture_unix_ms"].parse::<u64>().unwrap()
    );
}

#[tokio::test(start_paused = true)]
async fn hourly_checkpoint_matches_staged_preparation() {
    for financial in [false, true] {
        let (_dir, paths) = installed_fixture();
        append(
            &paths.source_log,
            activity_envelope("0xhourly-first", NOW_UNIX + 1),
        );
        // Financial mode must agree with the database used by the staged preparer.
        if financial {
            let paper = PaperStateDb::open(&paths.fixed_main).unwrap();
            let start = append_paper_record(&paths.paper_log, &start_record());
            paper
                .reset_financial_era(
                    start,
                    CollateralAmount::from_decimal_exact(dec!(10)).unwrap(),
                )
                .unwrap();
        }
        let mut opened = SourceLogBoot::open(&paths, financial).unwrap().unwrap();
        let slot = pe_service::source_checkpoint::CheckpointJobSlot::default();
        let mut owner = opened.boot.into_checkpoint_owner(slot.clone());
        owner.initialize_for_scenario().await.unwrap();
        let input = activity_envelope("0xhourly-second", NOW_UNIX + 2);
        let receipt = opened
            .sink
            .append_durable(activity_envelope("0xhourly-second", NOW_UNIX + 2))
            .unwrap();
        owner
            .record_synced_append_for_scenario(receipt, &input)
            .unwrap();
        tokio::time::advance(std::time::Duration::from_secs(
            pe_service::source_checkpoint::CHECKPOINT_PUBLISH_SECS,
        ))
        .await;
        owner.publish_hourly_for_scenario().await.unwrap();
        let hourly = checkpoint_json(&paths.source_log);
        drop(owner);
        drop(opened.sink);
        SourceLogBoot::prepare_checkpoint(&paths.fixed_main).unwrap();
        let prepared = checkpoint_json(&paths.source_log);
        for field in [
            "format_version",
            "scanner_version",
            "reducer_version",
            "financial_era",
            "activation",
            "tail",
            "receipt_count",
            "prefix_blake3",
        ] {
            assert_eq!(hourly[field], prepared[field], "{field}");
        }
        assert_eq!(hourly["activity"], prepared["activity"]);
        assert_eq!(hourly["daily_boundary"], prepared["daily_boundary"]);
    }
}

/// PASS: the typed quarantine failure exits 78 with either durable-quarantine fault and keeps
/// precedence over both shutdown-bound outcomes. Every row is fixed before running the binary.
#[tokio::test]
async fn checkpoint_invalidation_failure_preserves_status_78_at_shutdown_bounds() {
    for fault in [
        "PE_SCENARIO_CHECKPOINT_FAIL_QUARANTINE_RENAME",
        "PE_SCENARIO_CHECKPOINT_FAIL_QUARANTINE_SYNC",
    ] {
        for timeout in [
            None,
            Some("PE_SCENARIO_CHECKPOINT_SHUTDOWN_TIMEOUT"),
            Some("PE_SCENARIO_CHECKPOINT_JOB_JOIN_TIMEOUT"),
        ] {
            let fixture = checkpoint_rollout::CheckpointFixture::new().await;
            SourceLogBoot::prepare_checkpoint(&fixture.cfg.paper_state_db_path).unwrap();
            let pause = fixture.dir.path().join("prefix-pause");
            let mut env = vec![
                ("PE_SCENARIO_CHECKPOINT_PAUSE_BEFORE_HASH", pause.as_path()),
                (fault, Path::new("1")),
            ];
            if let Some(timeout) = timeout {
                env.push((timeout, Path::new("1")));
            }
            let child = checkpoint_rollout::Child::start_checkpoint(
                &fixture.config_path,
                Path::new(env!("CARGO_BIN_EXE_pe-service")),
                &env,
            );
            checkpoint_until(|| pause.with_extension("ready").exists()).await;
            corrupt_checkpoint_prefix(&fixture.cfg.source_event_log_path);
            std::fs::write(pause.with_extension("resume"), b"resume").unwrap();
            let output = child.finish_checkpoint(None).await;
            assert_eq!(
                output.status.code(),
                Some(78),
                "{fault} {timeout:?}: {output:?}"
            );
            let status: serde_json::Value =
                serde_json::from_slice(&std::fs::read(&fixture.cfg.status_path).unwrap()).unwrap();
            assert!(
                status
                    .to_string()
                    .contains("checkpoint_invalidation_failed"),
                "{status}"
            );
        }
    }
    checkpoint_unusable_lock_preserves_status_78().await;
}

async fn checkpoint_unusable_lock_preserves_status_78() {
    let fixture = checkpoint_rollout::CheckpointFixture::new().await;
    SourceLogBoot::prepare_checkpoint(&fixture.cfg.paper_state_db_path).unwrap();
    let pause = fixture.dir.path().join("prefix-pause");
    let child = checkpoint_rollout::Child::start_checkpoint(
        &fixture.config_path,
        Path::new(env!("CARGO_BIN_EXE_pe-service")),
        &[("PE_SCENARIO_CHECKPOINT_PAUSE_BEFORE_HASH", pause.as_path())],
    );
    checkpoint_until(|| pause.with_extension("ready").exists()).await;
    let lock = checkpoint_sidecar(&fixture.cfg.source_event_log_path, ".lock");
    std::fs::remove_file(&lock).unwrap();
    std::fs::create_dir(&lock).unwrap();
    corrupt_checkpoint_prefix(&fixture.cfg.source_event_log_path);
    std::fs::write(pause.with_extension("resume"), b"resume").unwrap();
    let output = child.finish_checkpoint(None).await;
    assert_eq!(output.status.code(), Some(78), "{output:?}");
    let status = std::fs::read_to_string(&fixture.cfg.status_path).unwrap();
    assert!(
        status.contains("checkpoint_invalidation_failed"),
        "{status}"
    );
}

#[tokio::test]
async fn checkpoint_quarantine_failure_after_owner_shutdown_preserves_status_78() {
    let fixture = checkpoint_rollout::CheckpointFixture::new().await;
    SourceLogBoot::prepare_checkpoint(&fixture.cfg.paper_state_db_path).unwrap();
    let pause = fixture.dir.path().join("prefix-pause");
    let child = checkpoint_rollout::Child::start_checkpoint(
        &fixture.config_path,
        Path::new(env!("CARGO_BIN_EXE_pe-service")),
        &[
            ("PE_SCENARIO_CHECKPOINT_PAUSE_BEFORE_HASH", pause.as_path()),
            (
                "PE_SCENARIO_CHECKPOINT_FAIL_QUARANTINE_RENAME",
                Path::new("1"),
            ),
        ],
    );
    checkpoint_until(|| pause.with_extension("ready").exists()).await;
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(checkpoint_sidecar(
            &fixture.cfg.source_event_log_path,
            ".lock",
        ))
        .unwrap();
    fs2::FileExt::lock_exclusive(&lock).unwrap();
    corrupt_checkpoint_prefix(&fixture.cfg.source_event_log_path);
    std::fs::write(pause.with_extension("resume"), b"resume").unwrap();
    fixture
        .wait_log("source checkpoint prefix invalid; invalidating")
        .await;
    child.signal_checkpoint("-INT");
    // TaskRunState::Stopped is written only after the supervisor joins the owner. The blocking
    // child remains held on our lock until this status snapshot proves CleanShutdown won.
    checkpoint_until(|| {
        std::fs::read(&fixture.cfg.status_path)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
            .is_some_and(|status| {
                status["tasks"].as_array().is_some_and(|tasks| {
                    tasks.iter().any(|task| {
                        task["name"] == "source_checkpoint" && task["state"] == "stopped"
                    })
                })
            })
    })
    .await;
    fs2::FileExt::unlock(&lock).unwrap();
    let output = child.finish_checkpoint(None).await;
    assert_eq!(output.status.code(), Some(78), "{output:?}");
}

/// PASS: an unreadable invalidation record stops the fence installation with status 78 (quiesced
/// recovery) before listening, never an ordinary failure that systemd restarts.
#[tokio::test]
async fn retention_fence_on_an_unreadable_invalidation_record_exits_78() {
    let fixture = checkpoint_rollout::CheckpointFixture::new().await;
    std::fs::write(
        checkpoint_sidecar(&fixture.cfg.source_event_log_path, ".invalidation"),
        b"{not json",
    )
    .unwrap();
    let output = boot_binary(&fixture.config_path, false).await;
    assert_eq!(output.status.code(), Some(78), "{output:?}");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("install source retention fence"),
        "{output:?}"
    );
    assert!(!fixture.logs().contains("pe-service listening"));
}

fn checkpoint_sidecar(source: &Path, suffix: &str) -> PathBuf {
    let mut path = pe_service::source_checkpoint::checkpoint_path(source).into_os_string();
    path.push(suffix);
    PathBuf::from(path)
}

#[tokio::test]
async fn crash_restart_from_hourly_checkpoint() {
    let fixture = checkpoint_rollout::CheckpointFixture::new().await;
    SourceLogBoot::prepare_checkpoint(&fixture.cfg.paper_state_db_path).unwrap();
    let child = checkpoint_rollout::Child::start_checkpoint(
        &fixture.config_path,
        Path::new(env!("CARGO_BIN_EXE_pe-service")),
        &[(
            "PE_SCENARIO_CHECKPOINT_PUBLISH_INTERVAL_MS",
            Path::new("500"),
        )],
    );
    checkpoint_until(|| {
        checkpoint_events(&fixture.logs(), "source checkpoint published").len() >= 2
    })
    .await;
    let output = child.finish_checkpoint(Some("-KILL")).await;
    assert!(!output.status.success());
    let events = checkpoint_events(&fixture.logs(), "source checkpoint published");
    let last = events.last().unwrap();
    let data = checkpoint_json(&fixture.cfg.source_event_log_path);
    let tail: pe_event_log::LogTailBinding = serde_json::from_value(data["tail"].clone()).unwrap();
    let lag =
        last["published_unix_ms"].as_u64().unwrap() - last["capture_unix_ms"].as_u64().unwrap();
    assert!(lag < 10_000);
    assert_eq!(
        data["prefix_blake3"],
        Scanner::hash_prefix(&tail.path, tail.physical_tail)
            .unwrap()
            .finalize()
            .to_hex()
            .as_str()
    );
    let complete = Scanner::verify(&tail.path).unwrap();
    std::fs::OpenOptions::new()
        .append(true)
        .open(&tail.path)
        .unwrap()
        .write_all(&[1, 2])
        .unwrap();
    fixture.clear_logs();
    let child = checkpoint_rollout::Child::start_checkpoint(
        &fixture.config_path,
        Path::new(env!("CARGO_BIN_EXE_pe-service")),
        &[],
    );
    fixture.wait_log("source checkpoint prefix verified").await;
    fixture.wait_log("source checkpoint published").await;
    let output = child.finish_checkpoint(Some("-INT")).await;
    assert!(output.status.success(), "{output:?}");
    let boot = &checkpoint_events(&fixture.logs(), "source checkpoint verification completed")[0];
    assert_eq!(boot["checkpoint_used"], true);
    assert_eq!(boot["checkpoint_offset"], tail.physical_tail);
    assert_eq!(
        boot["suffix_bytes"],
        complete.physical_tail - tail.physical_tail
    );
    assert!(file_len(&tail.path) >= complete.physical_tail);
}

/// Fixed declared outcomes: all four old-binary rows are allowed. Each must verify before serving,
/// reject the corrupt suffix control, retry idempotently and preserve finance/history/membership.
/// New-binary rows: absent/inactive use checkpoint; active full-walks and clears; unreadable always
/// full-walks until quiesced recovery, whose next full walk publishes a usable checkpoint.
#[tokio::test]
async fn previous_binary_rollback_matrix() {
    let Some(previous) = std::env::var_os("PE_ROLLBACK_SERVICE_BINARY") else {
        eprintln!(
            "SKIP previous_binary_rollback_matrix: PE_ROLLBACK_SERVICE_BINARY is unset; rollback compatibility is unproven"
        );
        return;
    };
    let previous = PathBuf::from(previous);
    assert!(previous.is_absolute());
    for binary in [
        Path::new(env!("CARGO_BIN_EXE_pe-service")),
        previous.as_path(),
    ] {
        let output = std::process::Command::new("sha256sum")
            .arg(binary)
            .output()
            .unwrap();
        assert!(output.status.success());
        eprintln!(
            "rollback executable identity: {}",
            String::from_utf8(output.stdout).unwrap().trim()
        );
        let version = std::process::Command::new(binary)
            .arg("--version")
            .output()
            .unwrap();
        assert!(version.status.success());
        eprintln!(
            "rollback embedded identity: {}",
            String::from_utf8(version.stdout).unwrap().trim()
        );
    }
    for row in ["absent", "inactive", "active", "unreadable"] {
        eprintln!("rollback row {row}: declared allowed; all preservation criteria required");
        let fixture = checkpoint_rollout::CheckpointFixture::new().await;
        // A genuine new-owner hourly artifact and persistent lock are the old reader's input.
        let child = checkpoint_rollout::Child::start_checkpoint(
            &fixture.config_path,
            Path::new(env!("CARGO_BIN_EXE_pe-service")),
            &[(
                "PE_SCENARIO_CHECKPOINT_PUBLISH_INTERVAL_MS",
                Path::new("500"),
            )],
        );
        fixture.copy();
        fixture.wait_copy().await;
        checkpoint_until(|| {
            checkpoint_events(&fixture.logs(), "source checkpoint published").len() >= 2
        })
        .await;
        let output = child.finish_checkpoint(Some("-INT")).await;
        assert!(output.status.success(), "{row}: {output:?}");
        fixture.mirror_positions();
        fixture.stale_anchors();
        let source = &fixture.cfg.source_event_log_path;
        let mut lock = pe_service::source_checkpoint::checkpoint_path(source).into_os_string();
        lock.push(".lock");
        assert!(PathBuf::from(lock).exists());
        let mut record = pe_service::source_checkpoint::checkpoint_path(source).into_os_string();
        record.push(".invalidation");
        let record = PathBuf::from(record);
        match row {
            "absent" => {
                if record.exists() {
                    std::fs::remove_file(&record).unwrap();
                }
            }
            "inactive" => std::fs::write(&record, br#"{"generation":4,"active":false}"#).unwrap(),
            "active" => std::fs::write(&record, br#"{"generation":4,"active":true}"#).unwrap(),
            "unreadable" => std::fs::write(&record, b"unreadable quarantine").unwrap(),
            _ => unreachable!(),
        }
        let record_before = std::fs::read(&record).ok();
        let finances = fixture.paper.financial_snapshot(NOW_UNIX).unwrap();
        let history = fixture.paper.gate_history().unwrap();
        let decisions = fixture.paper.decision_pending_history().unwrap();
        let source_before = std::fs::read(source).unwrap();
        // Control in every row: append a corrupt complete frame after the checkpoint.
        append(
            source,
            envelope("rollback.control", 1, 1, b"control", NOW_UNIX),
        );
        let mut corrupt = std::fs::read(source).unwrap();
        *corrupt.last_mut().unwrap() ^= 1;
        std::fs::write(source, corrupt).unwrap();
        fixture.clear_logs();
        let output = boot_binary_at(&fixture.config_path, &previous).await;
        assert!(
            !output.status.success(),
            "{row}: corrupt suffix unexpectedly allowed"
        );
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("CRC"),
            "{row}: {output:?}"
        );
        assert!(!fixture.logs().contains("pe-service listening"));
        assert_eq!(std::fs::read(&record).ok(), record_before);
        std::fs::write(source, &source_before).unwrap();
        fixture.clear_logs();
        std::fs::remove_file(&fixture.cfg.status_path).unwrap();
        let retry_start = file_len(source);
        // copy() serves transaction 1001; this is the same already-covered row, unchanged.
        let covered_hash = format!("0x{:064x}", 1001);
        let child =
            checkpoint_rollout::Child::start_checkpoint(&fixture.config_path, &previous, &[]);
        fixture.wait_log("pe-service listening").await;
        let logs = fixture.logs();
        let admitted = logs
            .lines()
            .position(|line| {
                let Ok(event) = serde_json::from_str::<serde_json::Value>(line) else {
                    return false;
                };
                let fields = event.get("fields").unwrap_or(&event);
                fields["message"] == "wallet bracket completed"
                    && fields["wallet"] == fixture.wallet().to_string()
                    && fields["outcome"] == "accepted"
            })
            .expect("previous binary must admit the stale-anchor fixture wallet");
        let listening = logs
            .lines()
            .position(|line| line.contains("pe-service listening"))
            .unwrap();
        assert!(
            admitted < listening,
            "{row}: previous-binary boot admission must precede listening"
        );
        // The previous poller records each reconciliation page before processing it, and
        // persists the wallet's feed-history frontier only after that read's buckets and
        // retirements are acknowledged. A frontier citing a page with this exact retry,
        // appended after this run began, proves the retry was processed successfully.
        checkpoint_until(|| {
            let Ok(frames) = pe_event_log::Reader::replay_with_offsets(source) else {
                return false;
            };
            let covered = frames
                .filter_map(Result::ok)
                .filter(|(offset, _, frame)| {
                    *offset >= retry_start
                        && frame.source_id.0 == "polymarket-public.activity-reconciliation"
                        && serde_json::from_slice::<serde_json::Value>(&frame.payload)
                            .ok()
                            .is_some_and(|rows| {
                                rows.as_array().is_some_and(|rows| {
                                    rows.iter()
                                        .any(|row| row["transactionHash"] == covered_hash)
                                })
                            })
                })
                .map(|(_, sequence, frame)| (sequence.0, frame.this_hash.to_hex().to_string()))
                .collect::<std::collections::HashSet<_>>();
            let Ok(collection) = fixture.paper.feed_history_frontiers() else {
                return false;
            };
            collection["frontiers"].as_array().is_some_and(|frontiers| {
                frontiers.iter().any(|frontier| {
                    frontier["wallet"] == fixture.wallet().to_string()
                        && frontier["page_occurrences"]
                            .as_array()
                            .is_some_and(|pages| {
                                pages.iter().any(|page| {
                                    let receipt = &page["receipt"];
                                    receipt["sequence"].as_u64().is_some_and(|sequence| {
                                        receipt["this_hash"].as_str().is_some_and(|hash| {
                                            covered.contains(&(sequence, hash.to_owned()))
                                        })
                                    })
                                })
                            })
                })
            })
        })
        .await;
        checkpoint_until(|| {
            std::fs::read(&fixture.cfg.status_path)
                .ok()
                .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
                .is_some_and(|status| {
                    status["uptime_secs"]
                        .as_u64()
                        .is_some_and(|seconds| seconds >= 2)
                })
        })
        .await;
        fixture.assert_live_membership();
        let output = child.finish_checkpoint(Some("-INT")).await;
        assert!(output.status.success(), "{row}: {output:?}");
        let logs = fixture.logs();
        assert!(
            logs.find("source checkpoint raw prefix verified").unwrap()
                < logs.find("pe-service listening").unwrap()
        );
        assert_eq!(
            std::fs::read(&record).ok(),
            record_before,
            "old binary changed {row} record"
        );
        assert_eq!(
            fixture.paper.financial_snapshot(NOW_UNIX).unwrap(),
            finances
        );
        assert_eq!(fixture.paper.gate_history().unwrap(), history);
        assert_eq!(fixture.paper.decision_pending_history().unwrap(), decisions);
        assert!(!fixture.paper.is_wallet_fenced(&fixture.wallet()).unwrap());
        assert!(
            fixture
                .paper
                .wallet_history_complete(&fixture.wallet())
                .unwrap()
        );
        fixture.clear_logs();
        let child = checkpoint_rollout::Child::start_checkpoint(
            &fixture.config_path,
            Path::new(env!("CARGO_BIN_EXE_pe-service")),
            &[],
        );
        fixture.wait_log("pe-service listening").await;
        if row != "unreadable" {
            fixture.wait_log("source checkpoint published").await;
        }
        let output = child.finish_checkpoint(Some("-INT")).await;
        assert!(output.status.success(), "{row}: new reader {output:?}");
        let boot =
            &checkpoint_events(&fixture.logs(), "source checkpoint verification completed")[0];
        assert_eq!(
            boot["checkpoint_used"],
            matches!(row, "absent" | "inactive"),
            "{row}"
        );
        if row == "active" {
            assert!(
                pe_service::source_checkpoint::read_authority(source)
                    .unwrap()
                    .permits_checkpoint()
            );
        }
        if row == "unreadable" {
            assert_eq!(std::fs::read(&record).ok(), record_before);
            fixture.clear_logs();
            let child = checkpoint_rollout::Child::start_checkpoint(
                &fixture.config_path,
                Path::new(env!("CARGO_BIN_EXE_pe-service")),
                &[],
            );
            fixture.wait_log("pe-service listening").await;
            assert!(
                !checkpoint_events(&fixture.logs(), "source checkpoint verification completed")[0]
                    ["checkpoint_used"]
                    .as_bool()
                    .unwrap()
            );
            assert!(child.finish_checkpoint(Some("-INT")).await.status.success());
            let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_pe-service"));
            command
                .env_clear()
                .arg("--recover-source-checkpoint")
                .arg("--paper-state")
                .arg(&fixture.cfg.paper_state_db_path);
            assert!(
                support::bounded_command_output(command)
                    .await
                    .status
                    .success()
            );
            fixture.clear_logs();
            let child = checkpoint_rollout::Child::start_checkpoint(
                &fixture.config_path,
                Path::new(env!("CARGO_BIN_EXE_pe-service")),
                &[],
            );
            fixture.wait_log("source checkpoint published").await;
            assert_eq!(
                checkpoint_events(&fixture.logs(), "source checkpoint verification completed")[0]["prefix_verification"],
                "full_walk"
            );
            assert!(child.finish_checkpoint(Some("-INT")).await.status.success());
            fixture.clear_logs();
            let child = checkpoint_rollout::Child::start_checkpoint(
                &fixture.config_path,
                Path::new(env!("CARGO_BIN_EXE_pe-service")),
                &[],
            );
            fixture.wait_log("source checkpoint prefix verified").await;
            assert!(child.finish_checkpoint(Some("-INT")).await.status.success());
            assert_eq!(
                checkpoint_events(&fixture.logs(), "source checkpoint verification completed")[0]["checkpoint_used"],
                true
            );
        }
        assert_eq!(
            fixture.paper.financial_snapshot(NOW_UNIX).unwrap(),
            finances
        );
        assert_eq!(fixture.paper.gate_history().unwrap(), history);
        assert_eq!(fixture.paper.decision_pending_history().unwrap(), decisions);
        eprintln!(
            "rollback row {row}: allowed PASS; corrupt control refused; retry idempotent; drained; balances/history/decisions/fences preserved; new invalidation policy PASS"
        );
    }
}

async fn boot_binary_at(config: &Path, binary: &Path) -> std::process::Output {
    let mut command = std::process::Command::new(binary);
    command
        .env_clear()
        .current_dir(config.parent().unwrap())
        .arg(config);
    support::bounded_command_output(command).await
}

fn damage_checkpoint_receipt(source: &Path, index: usize, edit: impl FnOnce(&mut [u8])) {
    let path = checkpoint_sidecar(source, ".receipts");
    let mut bytes = std::fs::read(&path).unwrap();
    let record = &mut bytes[index * 80..(index + 1) * 80];
    edit(&mut record[..48]);
    let checksum = blake3::hash(&record[..48]);
    record[48..].copy_from_slice(checksum.as_bytes());
    std::fs::write(path, bytes).unwrap();
}

fn corrupt_checkpoint_prefix(source: &Path) {
    use std::io::{Seek, SeekFrom};
    let offset = checkpoint_json(source)["tail"]["physical_tail"]
        .as_u64()
        .unwrap()
        - 1;
    let bytes = std::fs::read(source).unwrap();
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .open(source)
        .unwrap();
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(&[bytes[usize::try_from(offset).unwrap()] ^ 1])
        .unwrap();
    file.sync_all().unwrap();
}

/// Required reanchors retain progressive and Start-only boot wave behavior under a held bracket.
/// A stale reusable anchor stays live at listening with either membership shape, including with
/// status ticks disabled. No production endpoint is contacted.
#[tokio::test]
async fn progressive_boot_skips_waves_after_membership_record() {
    use axum::{Json, Router, extract::State, http::Uri, routing::any};
    use std::io::BufRead;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    #[derive(Clone)]
    struct BootAdmissionSource {
        start: AppendReceipt,
        now: i64,
        first_activity: Arc<AtomicBool>,
        activity_requested: Arc<tokio::sync::Notify>,
        release_activity: Arc<tokio::sync::Semaphore>,
    }
    async fn respond(
        State(source): State<BootAdmissionSource>,
        uri: Uri,
        body: axum::body::Bytes,
    ) -> Json<serde_json::Value> {
        use serde_json::json;
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
                ];
                json!(rows.map(|(key, value, value_type)| json!({"key": key, "value": value, "value_type": value_type})))
            }
            "/rest/v1/ranking_batches" => json!([{"batch_id":572}]),
            "/rest/v1/ranking_entries" | "/rest/v1/latest_ranking" => json!([
                {"batch_id":572,"rank":1,"wallet_hex":WALLET,"ls_tstat":"3","hit_rate":"0.6","n_trades":20,"last_trade_unix":source.now-60,"survives":true}
            ]),
            "/rest/v1/paper_bankroll" => json!([{"bankroll_str":"10","last_prepared_seq":null}]),
            "/rest/v1/paper_positions" | "/rest/v1/accounts" | "/rest/v1/account_credentials" => {
                json!([])
            }
            "/rest/v1/rpc/seed_financial_start" => {
                json!({"outcome":"existing", "start_seq":source.start.sequence.0, "start_hash":source.start.this_hash.to_hex().to_string()})
            }
            "/rest/v1/service_runtime" => {
                json!([{"watchlist_size":0,"updated_at":"fixture-token"}])
            }
            "/rest/v1/rpc/service_watchlist_replace_v1" => {
                let request: serde_json::Value = serde_json::from_slice(&body).unwrap();
                json!([{"new_token":"fixture-token","count":request["entries"].as_array().unwrap().len()}])
            }
            "/activity" => {
                if !source.first_activity.swap(true, Ordering::SeqCst) {
                    source.activity_requested.notify_one();
                }
                source.release_activity.acquire().await.unwrap().forget();
                json!([])
            }
            "/positions" => json!([]),
            _ => panic!("unexpected progressive-boot request {uri}"),
        })
    }

    struct BootChild(std::process::Child);
    impl Drop for BootChild {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    async fn status_at(path: &Path, expected: serde_json::Value) -> serde_json::Value {
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                if let Ok(bytes) = std::fs::read(path)
                    && let Ok(value) = serde_json::from_slice::<serde_json::Value>(&bytes)
                    && value["live_wallets"] == expected
                {
                    break value;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap()
    }

    for (post_start_record, reanchor_required) in
        [(true, true), (false, true), (true, false), (false, false)]
    {
        let (dir, mut paths) = version_one_fixture();
        paths.binary_identity = pe_service::build_info::embedded()
            .source_revision
            .to_owned();
        let paths = install_generation(paths);
        let wallet = WalletAddress::from_hex(WALLET).unwrap();
        let start = append_paper_record(&paths.paper_log, &start_record());
        let now = OffsetDateTime::now_utc().unix_timestamp();
        let paper = PaperStateDb::open(&paths.fixed_main).unwrap();
        paper
            .record_reconciled_history_status(&pe_paper_state::WalletHistoryStatusRecord {
                wallet,
                complete: true,
                proof_json: "{}".to_owned(),
                updated_at_unix: now,
            })
            .unwrap();
        support::install_full_history_anchor(&paper, wallet, now - 3_601);
        if reanchor_required {
            rusqlite::Connection::open(&paths.fixed_main)
                .unwrap()
                .execute(
                    "UPDATE poll_cursors SET reanchor_required = 1 WHERE wallet_hex = ?1",
                    rusqlite::params![wallet.to_string()],
                )
                .unwrap();
        }
        paper
            .reset_financial_era(
                start,
                CollateralAmount::from_decimal_exact(dec!(10)).unwrap(),
            )
            .unwrap();
        if post_start_record {
            let config = append(&paths.source_log, envelope("pe-service.watchlist-capacity-config", 1, 1,
                &serde_json::to_vec(&serde_json::json!({"generation":2,"target":1,"published_entries":start_batch().entries})).unwrap(), now));
            append_paper_record(&paths.paper_log, &pe_service::paper_recovery::MembershipChange {
                reason:pe_service::paper_recovery::MembershipReason::CapacityChange,
                removed:Vec::new(), added:Vec::new(), capacity:1, ranking_batch_id:None,
                evidence:serde_json::json!({"kind":"capacity_change","generation":2,"config_receipt":config,"admission_receipts":[]}),
            }.into_record());
        }
        // Preserve an already sealed historical generation; this scenario isolates admission.
        append_paper_record(
            &paths.paper_log,
            &PaperLogRecord::QualificationSealed(Box::new(
                pe_service::paper_recovery::QualificationSealed {
                    start_receipt: start,
                    source_prefix: TailBinding::from(&Scanner::verify(&paths.source_log).unwrap()),
                    financial_prefix: TailBinding::from(
                        &Scanner::verify(&paths.paper_log).unwrap(),
                    ),
                    live_prefix: TailBinding::from(&Scanner::verify(&paths.live_journal).unwrap()),
                    decision_evidence_digest: blake3::hash(b"[]").to_hex().to_string(),
                    sealed_cutoff_unix: now,
                    reason: pe_service::paper_recovery::SealReason::InsufficientEvidence(
                        "admission fixture".to_owned(),
                    ),
                },
            )),
        );
        drop(paper);
        let source = BootAdmissionSource {
            start,
            now,
            first_activity: Arc::new(AtomicBool::new(false)),
            activity_requested: Arc::new(tokio::sync::Notify::new()),
            release_activity: Arc::new(tokio::sync::Semaphore::new(0)),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let router = Router::new()
            .fallback(any(respond))
            .with_state(source.clone());
        let server = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async {
                    let _ = stopped.await;
                })
                .await
                .unwrap()
        });
        let cfg = pe_service::config::ServiceConfig {
            bind: "127.0.0.1:0".to_owned(),
            paper_state_db_path: paths.fixed_main.clone(),
            source_event_log_path: paths.source_log.clone(),
            event_log_path: paths.paper_log.clone(),
            legacy_wallet_history_path: paths.legacy_history.clone(),
            jsonl_log_path: dir.path().join("service.jsonl"),
            status_path: dir.path().join("status.json"),
            status_interval_secs: u64::from(reanchor_required),
            supabase_url: base.clone(),
            supabase_secret_key: "fixture".to_owned(),
            supabase_authoritative: true,
            polymarket_base_url: base.clone(),
            gamma_base_url: base.clone(),
            polymarket_clob_base_url: base.clone(),
            polygon_receipt_rpc_url: base,
            bankroll_usd: "10".to_owned(),
            maintenance_interval_secs: 600,
            ..Default::default()
        };
        let config_path = dir.path().join("service.toml");
        std::fs::write(&config_path, toml::to_string(&cfg).unwrap()).unwrap();
        let mut command = boot_binary_command(&config_path, false);
        command
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        let mut child = BootChild(command.spawn().unwrap());
        let (lines, mut received) = tokio::sync::mpsc::channel(256);
        let pipes: Vec<Box<dyn std::io::Read + Send>> = vec![
            Box::new(child.0.stdout.take().unwrap()),
            Box::new(child.0.stderr.take().unwrap()),
        ];
        let readers = pipes
            .into_iter()
            .map(|pipe| {
                let lines = lines.clone();
                tokio::task::spawn_blocking(move || {
                    for line in std::io::BufReader::new(pipe).lines() {
                        if lines.blocking_send(line.unwrap()).is_err() {
                            break;
                        }
                    }
                })
            })
            .collect::<Vec<_>>();
        drop(lines);
        let mut captured = Vec::new();
        if !post_start_record && reanchor_required {
            tokio::time::timeout(
                Duration::from_secs(20),
                source.activity_requested.notified(),
            )
            .await
            .unwrap();
            while let Ok(line) = received.try_recv() {
                captured.push(line);
            }
            assert!(
                captured
                    .iter()
                    .all(|line: &String| !line.contains("pe-service listening"))
            );
            assert!(
                !cfg.status_path.exists(),
                "Start without membership record waits for boot waves"
            );
            source.release_activity.add_permits(1_000);
        }
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                let line = received
                    .recv()
                    .await
                    .expect("binary exited before listening");
                let listening = line.contains("pe-service listening");
                captured.push(line);
                if listening {
                    break;
                }
            }
        })
        .await
        .expect("binary did not listen while the runtime bracket was held");
        if post_start_record && reanchor_required {
            let zero = status_at(&cfg.status_path, serde_json::json!([])).await;
            assert_eq!(zero["watchlist_size"], 0);
            assert!(zero["live_wallets_at_unix_ms"].as_i64().is_some());
            assert!(
                captured
                    .iter()
                    .any(|line| line.contains("zero eligible live wallets"))
            );
            source.release_activity.add_permits(1_000);
        }
        let listening = captured
            .iter()
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .find(|line| line["message"] == "pe-service listening")
            .unwrap();
        let expected = if post_start_record && reanchor_required {
            Vec::new()
        } else {
            vec![wallet.to_string()]
        };
        assert_eq!(listening["live_wallets"], expected.len());
        let wallets: Vec<String> =
            serde_json::from_str(listening["live_wallet_list"].as_str().unwrap()).unwrap();
        assert_eq!(wallets, expected);
        source.release_activity.add_permits(1_000);
        if reanchor_required {
            let admitted =
                status_at(&cfg.status_path, serde_json::json!([wallet.to_string()])).await;
            assert_eq!(admitted["watchlist_size"], 1);
        }
        // Listening/status can precede main's shutdown loop while it publishes the boot
        // checkpoint. On Linux, wait for the actual SIGINT handler rather than a delay.
        #[cfg(target_os = "linux")]
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                let status =
                    std::fs::read_to_string(format!("/proc/{}/status", child.0.id())).unwrap();
                let caught = status
                    .lines()
                    .find_map(|line| line.strip_prefix("SigCgt:"))
                    .map(|mask| u64::from_str_radix(mask.trim(), 16).unwrap())
                    .unwrap();
                if caught & 2 != 0 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("binary did not install its shutdown handler");
        assert!(
            std::process::Command::new("kill")
                .arg("-INT")
                .arg(child.0.id().to_string())
                .status()
                .unwrap()
                .success()
        );
        tokio::time::timeout(Duration::from_secs(20), async {
            while let Some(line) = received.recv().await {
                captured.push(line);
            }
        })
        .await
        .unwrap();
        let exit = child.0.wait().unwrap();
        assert!(exit.success(), "{exit}: {}", captured.join("\n"));
        for reader in readers {
            reader.await.unwrap();
        }
        let logs = captured
            .iter()
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .collect::<Vec<_>>();
        assert!(
            logs.iter()
                .any(|line| line["message"] == "boot anchor selection census"
                    && line["reused"] == usize::from(!reanchor_required)
                    && line["walked"] == usize::from(reanchor_required))
        );
        if !reanchor_required {
            assert!(
                logs.iter()
                    .any(|line| line["message"] == "disk space monitoring started")
            );
            let status: serde_json::Value =
                serde_json::from_slice(&std::fs::read(&cfg.status_path).unwrap()).unwrap();
            assert!(
                status["tasks"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|task| task["name"] == "disk_monitor" && task["state"] == "stopped")
            );
        } else if post_start_record {
            assert!(
                logs.iter()
                    .any(|line| line["message"] == "admission launch order"
                        && line["path"] == "reentry"
                        && line["started"] == 1)
            );
        } else {
            let listening = captured
                .iter()
                .position(|line| line.contains("pe-service listening"))
                .unwrap();
            assert!(
                captured[..listening]
                    .iter()
                    .any(|line| line.contains("wallet bracket completed"))
            );
        }
        stop.send(()).unwrap();
        server.await.unwrap();
    }
}

#[tokio::test]
async fn runtime_checkpoint_prunes_only_after_successful_disposition_barrier() {
    for blocked in [false, true] {
        let (_dir, paths) = installed_fixture();
        let retired = append(
            &paths.source_log,
            activity_envelope("0xretired", NOW_UNIX + 1),
        );
        let disposed = append(
            &paths.source_log,
            activity_envelope("0xdisposed", NOW_UNIX + 2),
        );
        let surviving = append(
            &paths.source_log,
            activity_envelope("0xsurviving", NOW_UNIX + 3),
        );
        let retired_id = pe_source_polymarket_public::parse_activity_trade_observation(
            &activity_payload("0xretired", NOW_UNIX + 1),
        )
        .unwrap()
        .group_id
        .key()
        .clone();
        // An appended but unauthenticated commitment cannot override exact durable retirement.
        let binding = pe_service::bucket_commit::ObservationBinding {
            stream_group_id: retired_id.clone(),
            stream_receipt: retired,
            history_group_id: retired_id,
            semantic_revision: "recorded".to_owned(),
            page_raw_hash: "recorded".to_owned(),
            page_occurrence_index: 0,
            identity_provenance: None,
            identity_receipt: None,
            counterpart_basis_receipt: None,
            frame_admission_receipt: None,
        };
        let commitment = append(
            &paths.source_log,
            envelope(
                pe_service::bucket_commit::ACTIVITY_READ_COMMITMENT_SOURCE_ID,
                2,
                1,
                &serde_json::to_vec(&pe_service::bucket_commit::ActivityReadCommitment {
                    version: 2,
                    wallet: WalletAddress::from_hex(WALLET).unwrap(),
                    fixed_end: NOW_UNIX + 4,
                    digest: "unauthenticated".to_owned(),
                    bindings: Some(vec![binding]),
                    read_proof: None,
                })
                .unwrap(),
                NOW_UNIX + 4,
            ),
        );
        let paper = Arc::new(PaperStateDb::open(&paths.fixed_main).unwrap());
        let reader = Connection::open(&paths.fixed_main).unwrap();
        if blocked {
            reader
                .execute_batch("BEGIN; SELECT count(*) FROM meta;")
                .unwrap();
        }
        paper.retire_activity_observation(retired, false).unwrap();
        let observation = pe_source_polymarket_public::parse_activity_trade_observation(
            &activity_payload("0xdisposed", NOW_UNIX + 2),
        )
        .unwrap();
        paper
            .commit_activity_bucket(&pe_paper_state::ActivityBucketCommit {
                wallet: observation.wallet,
                source_epoch: NOW_UNIX + 2,
                dispositions: vec![pe_paper_state::ActivityDispositionRecord {
                    source_trade_id: observation.group_id.key().clone(),
                    transaction_hash: observation.group_id.components().transaction_hash.clone(),
                    wallet: observation.wallet,
                    source_epoch: NOW_UNIX + 2,
                    semantic_revision: "disposed".to_owned(),
                    activity_type: "TRADE".to_owned(),
                    disposition: "raw_only".to_owned(),
                    proof_json: "{}".to_owned(),
                    no_copy: None,
                }],
                leader_positions: Vec::new(),
                gate_results: Vec::new(),
                history_effects: Vec::new(),
                history_status: None,
                pending: Vec::new(),
                fence: None,
                reanchor: None,
                advance_cursor: false,
            })
            .unwrap();
        // Preparation is read-only and retains both disposed candidates and their commitment.
        SourceLogBoot::prepare_checkpoint(&paths.fixed_main).unwrap();
        let unpruned = checkpoint_json(&paths.source_log);
        assert_eq!(
            unpruned["activity"]["binding_commitments"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        let candidates = |data: &serde_json::Value| {
            data["activity"]["by_wallet"]
                .as_object()
                .unwrap()
                .values()
                .flat_map(|epochs| epochs.as_object().unwrap().values())
                .flat_map(|groups| groups.as_object().unwrap().values())
                .map(|obligation| {
                    serde_json::from_value::<AppendReceipt>(obligation["receipt"].clone()).unwrap()
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(candidates(&unpruned), vec![retired, disposed, surviving]);
        rewrite_boot_checkpoint(
            &pe_service::source_checkpoint::checkpoint_path(&paths.source_log),
            |data| {
                let receipts: Vec<_> = pe_event_log::Reader::replay_with_offsets(&paths.source_log).unwrap()
                    .map(|item| {
                        let (offset, sequence, envelope) = item.unwrap();
                        serde_json::json!({
                            "receipt": AppendReceipt { sequence, this_hash: envelope.this_hash },
                            "received_millis": i64::try_from(envelope.received_at.0.unix_timestamp_nanos() / 1_000_000).unwrap(),
                            "byte_offset": offset,
                        })
                    }).collect();
                data["format_version"] = serde_json::json!(1);
                data["receipts"] = serde_json::json!(receipts);
                data.as_object_mut().unwrap().remove("receipt_count");
                data.as_object_mut().unwrap().remove("generation");
                data["reducer_version"] = serde_json::json!(2);
                data["activity"]
                    .as_object_mut()
                    .unwrap()
                    .remove("recorded_bindings");
            },
        );
        std::fs::remove_file(checkpoint_sidecar(&paths.source_log, ".receipts")).unwrap();
        let mut converted = SourceLogBoot::open(&paths, false).unwrap().unwrap();
        assert_eq!(
            converted
                .boot
                .receipt_index()
                .read_verification_count(commitment),
            0
        );
        assert_eq!(checkpoint_json(&paths.source_log)["format_version"], 2);
        converted.boot.extend(&mut converted.sink).unwrap();
        assert_eq!(
            converted
                .boot
                .obligations(&paper, &paths.paper_log)
                .unwrap()
                .unresolved_receipts(WalletAddress::from_hex(WALLET).unwrap()),
            vec![surviving]
        );
        drop(converted);
        SourceLogBoot::prepare_checkpoint(&paths.fixed_main).unwrap();
        let converted = checkpoint_json(&paths.source_log);
        assert_eq!(converted["reducer_version"], 3);
        assert_eq!(candidates(&converted), vec![retired, disposed, surviving]);
        assert_eq!(
            converted["activity"]["recorded_bindings"][0]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        let previous = std::fs::read(pe_service::source_checkpoint::checkpoint_path(
            &paths.source_log,
        ))
        .unwrap();
        let opened = SourceLogBoot::open(&paths, false).unwrap().unwrap();
        let index = opened.boot.receipt_index();
        let slot = pe_service::source_checkpoint::CheckpointJobSlot::default();
        let mut owner = opened
            .boot
            .into_checkpoint_owner(slot.clone())
            .with_paper_state(paper.clone());
        owner.initialize_for_scenario().await.unwrap();
        if blocked {
            assert_eq!(
                std::fs::read(pe_service::source_checkpoint::checkpoint_path(
                    &paths.source_log
                ))
                .unwrap(),
                previous
            );
            reader.execute_batch("ROLLBACK").unwrap();
            owner.publish_hourly_for_scenario().await.unwrap();
        }
        let pruned = checkpoint_json(&paths.source_log);
        assert_eq!(candidates(&pruned), vec![surviving]);
        assert_eq!(
            pruned["activity"]["frame_candidates"]
                .as_object()
                .unwrap()
                .len(),
            1
        );
        assert!(
            pruned["activity"]["binding_commitments"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        assert_eq!(index.read_verification_count(commitment), 0);
        paper.sync_checkpoint_dispositions().unwrap();
        drop(owner);
        slot.join().await.unwrap();
        drop(opened.sink);
        let mut reboot = SourceLogBoot::open(&paths, false).unwrap().unwrap();
        reboot.boot.extend(&mut reboot.sink).unwrap();
        assert_eq!(
            reboot
                .boot
                .obligations(&paper, &paths.paper_log)
                .unwrap()
                .unresolved_receipts(WalletAddress::from_hex(WALLET).unwrap()),
            vec![surviving]
        );
    }
}

/// Install only the authority and dense receipt metadata; erasure is a separate step.
fn retention_capture(
    source: &Path,
    boundary_sequence: u64,
    pins: &[(AppendReceipt, bool)],
) -> pe_event_log::RetentionAuthority {
    let tail = Scanner::verify(source).unwrap();
    let frames: Vec<_> = pe_event_log::Reader::replay_with_offsets(source)
        .unwrap()
        .map(Result::unwrap)
        .collect();
    let boundary_index = usize::try_from(boundary_sequence).unwrap();
    let mut receipts = std::fs::File::create(checkpoint_sidecar(source, ".receipts")).unwrap();
    for (offset, sequence, envelope) in &frames {
        receipts
            .write_all(
                &pe_event_log::ReceiptRecord {
                    receipt: AppendReceipt {
                        sequence: *sequence,
                        this_hash: envelope.this_hash,
                    },
                    received_millis: i64::try_from(
                        envelope.received_at.0.unix_timestamp_nanos() / 1_000_000,
                    )
                    .unwrap(),
                    byte_offset: Some(*offset),
                }
                .encode(),
            )
            .unwrap();
    }
    receipts.sync_all().unwrap();
    let mut retained: Vec<_> = pins
        .iter()
        .map(|(receipt, reducer)| {
            let (offset, _, envelope) = &frames[usize::try_from(receipt.sequence.0).unwrap()];
            assert_eq!(receipt.this_hash, envelope.this_hash);
            pe_event_log::RetentionPin {
                sequence: receipt.sequence,
                offset: *offset,
                hash: receipt.this_hash,
                predecessor_hash: envelope.prev_hash,
                reducer: *reducer,
                wallet: if *reducer && envelope.source_id.0 == ACTIVITY_WS_SOURCE_ID {
                    Some(
                        pe_source_polymarket_public::parse_activity_trade_observation(
                            &envelope.payload,
                        )
                        .unwrap()
                        .wallet,
                    )
                } else {
                    None
                },
            }
        })
        .collect();
    retained.sort_by_key(|pin| pin.sequence);
    let authority = pe_event_log::RetentionAuthority {
        format_version: 1,
        epoch: 1,
        advanced_at: NOW_UNIX + 8 * 86_400,
        boundary: pe_event_log::RetentionBoundary {
            sequence: EventSeq(boundary_sequence),
            offset: frames[boundary_index].0,
        },
        chain_head: frames[boundary_index - 1].2.this_hash,
        pins: retained,
        retained_tail: (&tail).into(),
        feed: Vec::new(),
    };
    authority.write(source).unwrap();
    authority
}

fn erase_retention_frames(source: &Path, authority: &pe_event_log::RetentionAuthority) {
    use std::io::{Read as _, Seek as _, SeekFrom};
    let mut receipts = std::fs::File::open(checkpoint_sidecar(source, ".receipts")).unwrap();
    let records: Vec<_> = (0..=authority.boundary.sequence.0)
        .map(|sequence| {
            pe_event_log::ReceiptRecord::read(&mut receipts, EventSeq(sequence)).unwrap()
        })
        .collect();
    let header = std::fs::read(source).unwrap()[..5].to_vec();
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(source)
        .unwrap();
    for sequence in 0..authority.boundary.sequence.0 {
        if authority.pin(EventSeq(sequence)).is_some() {
            continue;
        }
        let i = usize::try_from(sequence).unwrap();
        let start = records[i].byte_offset.unwrap();
        let length = records[i + 1].byte_offset.unwrap() - start;
        file.seek(SeekFrom::Start(start)).unwrap();
        file.write_all(&vec![0; usize::try_from(length).unwrap()])
            .unwrap();
    }
    file.sync_all().unwrap();
    file.rewind().unwrap();
    let mut actual_header = vec![0; 5];
    file.read_exact(&mut actual_header).unwrap();
    assert_eq!(actual_header, header);
}

#[test]
fn retention_exact_read_rechecks_commit_before_punch_shared_block_and_failed_io() {
    use pe_service::risk_inputs::RiskInputsUnavailable;
    for physical_case in ["before_punch", "shared_block", "after_punch"] {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source.log");
        let erased = append(&source, envelope("old", 1, 1, b"{}", NOW_UNIX));
        if physical_case != "shared_block" {
            append(
                &source,
                envelope("padding", 1, 1, &retention_padding(), NOW_UNIX),
            );
        }
        let pinned = append(&source, envelope("pin", 1, 1, b"{}", NOW_UNIX));
        append(&source, envelope("retained", 1, 1, b"{}", NOW_UNIX + 1));
        let index = SourceReceiptIndex::replay(&source).unwrap();
        let authority = retention_capture(&source, pinned.sequence.0 + 1, &[(pinned, false)]);
        assert_eq!(
            authority.pins[0].offset / 4096 == pe_event_log::HEADER_LEN / 4096,
            physical_case == "shared_block"
        );
        let outcome = index.source_envelope_with_pause(erased, &mut || {
            index.install_retention(authority.clone()).unwrap();
            if physical_case == "after_punch" {
                erase_retention_frames(&source, &authority);
            }
        });
        assert_eq!(
            outcome.unwrap_err(),
            RiskInputsUnavailable::Erased,
            "{physical_case}"
        );
        assert_eq!(
            index
                .source_envelope_with_pause(pinned, &mut || {})
                .unwrap()
                .this_hash,
            pinned.this_hash
        );
        assert_eq!(
            index.receipt_at(erased.sequence).unwrap().unwrap().0,
            erased
        );
        // The prefix remains durable metadata after the physical evidence has gone.
        erase_retention_frames(&source, &authority);
        let replay = SourceReceiptIndex::replay(&source).unwrap();
        assert_eq!(
            replay.receipt_at(erased.sequence).unwrap(),
            index.receipt_at(erased.sequence).unwrap()
        );
        assert_eq!(
            replay
                .source_envelope_with_pause(erased, &mut || {})
                .unwrap_err(),
            RiskInputsUnavailable::Erased
        );
    }
}

#[test]
fn retention_dependency_pin_categories_stay_readable_without_feeding_reducers() {
    let (_dir, paths, reducer_authority, baseline) = retention_recovery_fixture();
    let mut dependency_authority = reducer_authority.clone();
    for pin in &mut dependency_authority.pins {
        pin.reducer = false;
        pin.wallet = None;
    }
    dependency_authority.write(&paths.source_log).unwrap();
    erase_retention_frames(&paths.source_log, &dependency_authority);
    let paper = PaperStateDb::open(&paths.fixed_main).unwrap();
    let mut opened = SourceLogBoot::open(&paths, true).unwrap().unwrap();
    let index = opened.boot.receipt_index();
    let mut sources = std::collections::HashSet::new();
    for pin in &dependency_authority.pins {
        let envelope = index
            .source_envelope_with_pause(
                AppendReceipt {
                    sequence: pin.sequence,
                    this_hash: pin.hash,
                },
                &mut || {},
            )
            .unwrap();
        sources.insert(envelope.source_id.0);
    }
    assert!(sources.contains(ACTIVITY_WS_SOURCE_ID));
    assert!(sources.contains(pe_service::bucket_commit::ACTIVITY_READ_COMMITMENT_SOURCE_ID));
    assert!(sources.contains(DAILY_BOUNDARY_SOURCE_ID));
    opened.boot.extend(&mut opened.sink).unwrap();
    let obligations = opened.boot.obligations(&paper, &paths.paper_log).unwrap();
    assert!(obligations.is_empty());
    assert!(obligations.pending_boundary().is_none());
    assert!(obligations.frame_recovery_receipts().0.is_empty());
    drop(opened);
    reducer_authority.write(&paths.source_log).unwrap();
    let mut opened = SourceLogBoot::open(&paths, true).unwrap().unwrap();
    opened.boot.extend(&mut opened.sink).unwrap();
    assert_eq!(
        opened.boot.obligations(&paper, &paths.paper_log).unwrap(),
        baseline.with_boundary
    );
}

struct RetentionRecoveryBaseline {
    obligations: pe_service::trade_poller::ReconciliationObligations,
    with_boundary: pe_service::trade_poller::ReconciliationObligations,
    receipts: Vec<(AppendReceipt, i64)>,
}

fn retention_recovery_fixture() -> (
    tempfile::TempDir,
    PaperMigrationPaths,
    pe_event_log::RetentionAuthority,
    RetentionRecoveryBaseline,
) {
    let (dir, paths) = installed_fixture();
    let start = append_paper_record(&paths.paper_log, &start_record());
    PaperStateDb::open(&paths.fixed_main)
        .unwrap()
        .seed_financial_start(start)
        .unwrap();
    let first = append(
        &paths.source_log,
        activity_envelope("0xold-bound", NOW_UNIX + 1),
    );
    let second = append(
        &paths.source_log,
        activity_envelope("0xold-unbound", NOW_UNIX + 2),
    );
    let routed = append(
        &paths.source_log,
        envelope(
            pe_service::frame_admission::FRAME_FALLBACK_SOURCE_ID,
            1,
            1,
            &serde_json::to_vec(&pe_service::frame_admission::FrameFallbackArtifact {
                version: 1,
                frame_receipt: first,
                routing_clock: OffsetDateTime::from_unix_timestamp(NOW_UNIX + 3).unwrap(),
                reason: pe_service::frame_admission::FrameFallbackReason::HistoryBehind,
                frontier: None,
                latest_incident_basis: Default::default(),
            })
            .unwrap(),
            NOW_UNIX + 3,
        ),
    );
    let wallet = WalletAddress::from_hex(WALLET).unwrap();
    let payload = serde_json::to_vec(&serde_json::json!([{
        "proxyWallet": wallet, "timestamp": NOW_UNIX + 1, "conditionId": format!("0x{}", "11".repeat(32)),
        "type": "TRADE", "size": "100", "usdcSize": "50", "transactionHash": "0xold-bound",
        "price": "0.5", "asset": "123", "side": "BUY", "outcomeIndex": 0, "outcome": "Yes", "isCombo": false,
    }])).unwrap();
    let mut writer = Writer::open(&paths.source_log).unwrap();
    let (read, _) = support::append_committed_read_v2(
        &mut writer,
        wallet,
        &payload,
        NOW_UNIX + 4,
        NOW_UNIX + 5,
    );
    let pages =
        serde_json::from_str::<serde_json::Value>(&read.decision_inputs_json).unwrap()["pages"]
            .clone();
    let aggregate = &read.aggregates[0];
    let binding = pe_service::bucket_commit::ObservationBinding {
        stream_group_id: pe_source_polymarket_public::parse_activity_trade_observation(
            &activity_payload("0xold-bound", NOW_UNIX + 1),
        )
        .unwrap()
        .group_id
        .key()
        .clone(),
        stream_receipt: first,
        history_group_id: aggregate.group_id.key().clone(),
        semantic_revision: aggregate.semantic_revision.as_str().to_owned(),
        page_raw_hash: read.page.raw_hash.clone(),
        page_occurrence_index: 0,
        identity_provenance: None,
        identity_receipt: None,
        counterpart_basis_receipt: None,
        frame_admission_receipt: None,
    };
    let commitment = writer
        .append_synced(envelope(
            pe_service::bucket_commit::ACTIVITY_READ_COMMITMENT_SOURCE_ID,
            2,
            1,
            &pe_service::bucket_commit::activity_read_commitment_payload_v2(
                wallet,
                NOW_UNIX + 4,
                std::slice::from_ref(&read.page),
                &serde_json::from_value::<Vec<_>>(pages).unwrap(),
                &[binding],
            )
            .unwrap(),
            NOW_UNIX + 6,
        ))
        .unwrap();
    drop(writer);
    let daily = append(
        &paths.source_log,
        envelope(
            DAILY_BOUNDARY_SOURCE_ID,
            1,
            1,
            &serde_json::to_vec(
                &serde_json::json!({"kind":"daily_boundary","cutoff_unix":NOW_UNIX + 86_400}),
            )
            .unwrap(),
            NOW_UNIX + 7,
        ),
    );
    let padding = append(
        &paths.source_log,
        envelope("padding", 1, 1, b"{}", NOW_UNIX + 8),
    );
    append(
        &paths.source_log,
        envelope("suffix", 1, 1, b"{}", NOW_UNIX + 9),
    );
    append(
        &paths.source_log,
        envelope("suffix", 1, 1, b"{}", NOW_UNIX + 10),
    );
    SourceLogBoot::prepare_checkpoint(&paths.fixed_main).unwrap();
    let activation = recorded_source(&paths);
    let paper = PaperStateDb::open(&paths.fixed_main).unwrap();
    let index = SourceReceiptIndex::replay(&paths.source_log).unwrap();
    let obligations = rebuild_reconciliation_obligations(&paths.source_log, &paper).unwrap();
    let mut with_boundary = obligations.clone();
    pe_service::trade_poller::recover_daily_boundary(
        &paths.source_log,
        &paths.paper_log,
        &mut with_boundary,
    )
    .unwrap();
    assert_eq!(
        with_boundary.pending_boundary(),
        Some(pe_service::trade_poller::PendingBoundary {
            receipt: daily,
            cutoff_unix: NOW_UNIX + 86_400,
        })
    );
    let receipts = pe_event_log::Reader::replay(&paths.source_log)
        .unwrap()
        .map(|frame| index.receipt_at(frame.unwrap().0).unwrap().unwrap())
        .collect();
    let baseline = RetentionRecoveryBaseline {
        obligations,
        with_boundary,
        receipts,
    };
    let authority = retention_capture(
        &paths.source_log,
        padding.sequence.0 + 1,
        &[
            (
                AppendReceipt {
                    sequence: activation.last_sequence.unwrap(),
                    this_hash: activation.last_hash,
                },
                false,
            ),
            (first, true),
            (second, true),
            (routed, true),
            (read.page.receipt, false),
            (commitment, true),
            (daily, true),
        ],
    );
    (dir, paths, authority, baseline)
}

#[test]
fn retention_rebuild_checkpoint_loss_stale_epoch_poison_preparation_and_migration_agree() {
    let (_dir, paths, authority, baseline) = retention_recovery_fixture();
    let paper = PaperStateDb::open(&paths.fixed_main).unwrap();
    assert!(paper.decision_pending_history().unwrap().is_empty());
    let expected = baseline.obligations;
    assert_eq!(expected.len(), 2);
    erase_retention_frames(&paths.source_log, &authority);
    for checkpoint in [true, false] {
        if !checkpoint {
            std::fs::remove_file(pe_service::source_checkpoint::checkpoint_path(
                &paths.source_log,
            ))
            .unwrap();
        }
        let mut opened = SourceLogBoot::open(&paths, true).unwrap().unwrap();
        opened.boot.extend(&mut opened.sink).unwrap();
        let expected_with_boundary = &baseline.with_boundary;
        assert_eq!(
            &opened.boot.obligations(&paper, &paths.paper_log).unwrap(),
            expected_with_boundary
        );
        for (sequence, metadata) in baseline.receipts.iter().enumerate() {
            assert_eq!(
                opened
                    .boot
                    .receipt_index()
                    .receipt_at(EventSeq(u64::try_from(sequence).unwrap()))
                    .unwrap(),
                Some(*metadata)
            );
        }
        assert_eq!(expected_with_boundary.frame_recovery_receipts().1.len(), 1);
        opened
            .boot
            .prepare_installed(paths.clone(), NOW_UNIX + 20)
            .unwrap();
        opened.sink.poison_for_scenario();
        assert!(opened.sink.try_reopen());
        assert_eq!(
            rebuild_reconciliation_obligations(&paths.source_log, &paper).unwrap(),
            expected
        );
        assert_eq!(
            pe_service::trade_poller::rebuild_reconciliation_obligations_with_index(
                &paths.source_log,
                &paper,
                &opened.boot.receipt_index()
            )
            .unwrap(),
            expected
        );
    }
    let (prepared, validated) = SourceLogBoot::prepare_checkpoint(&paths.fixed_main).unwrap();
    assert_eq!(validated, 0);
    assert_eq!(
        prepared.tail.physical_tail,
        authority.retained_tail.physical_tail
    );
    assert_eq!(
        checkpoint_json(&paths.source_log)["activity"]["routed_frames"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let mut prepared_boot = SourceLogBoot::open(&paths, true).unwrap().unwrap();
    assert!(prepared_boot.boot.checkpoint_assisted_for_scenario());
    prepared_boot.boot.extend(&mut prepared_boot.sink).unwrap();
    assert_eq!(
        prepared_boot
            .boot
            .obligations(&paper, &paths.paper_log)
            .unwrap(),
        baseline.with_boundary
    );
    for (sequence, metadata) in baseline.receipts.iter().enumerate() {
        assert_eq!(
            prepared_boot
                .boot
                .receipt_index()
                .receipt_at(EventSeq(u64::try_from(sequence).unwrap()))
                .unwrap(),
            Some(*metadata)
        );
    }
    drop(prepared_boot);
    PaperMigrationBoot::prepare(paths.clone(), NOW_UNIX + 21).unwrap();
}

#[tokio::test]
async fn retention_matching_format_three_boot_defers_dependency_pin_failure() {
    let (_dir, paths, authority, _) = retention_recovery_fixture();
    erase_retention_frames(&paths.source_log, &authority);
    SourceLogBoot::prepare_checkpoint(&paths.fixed_main).unwrap();
    let opened = SourceLogBoot::open(&paths, true).unwrap().unwrap();
    assert!(opened.boot.checkpoint_assisted_for_scenario());
    assert_eq!(checkpoint_json(&paths.source_log)["format_version"], 3);
    assert_eq!(
        checkpoint_json(&paths.source_log)["retention_epoch"],
        authority.epoch
    );
    let pin = authority
        .pins
        .iter()
        .find(|pin| !pin.reducer && Some(pin.sequence) != recorded_source(&paths).last_sequence)
        .unwrap();
    let mut bytes = std::fs::read(&paths.source_log).unwrap();
    bytes[usize::try_from(pin.offset + 20).unwrap()] ^= 1;
    std::fs::write(&paths.source_log, bytes).unwrap();
    let slot = pe_service::source_checkpoint::CheckpointJobSlot::default();
    let mut owner = opened.boot.into_checkpoint_owner(slot.clone());
    drop(opened.sink);
    assert!(owner.initialize_for_scenario().await.is_err());
    assert!(!slot.quarantine_failed());
    assert!(!pe_service::source_checkpoint::checkpoint_path(&paths.source_log).exists());
    assert!(SourceLogBoot::open(&paths, true).is_err());
}

#[test]
fn retention_rebuild_refuses_missing_metadata_corrupt_dependency_and_committed_tail_cuts() {
    for fault in [
        "authority",
        "receipts",
        "receipt_checksum",
        "dependency",
        "activation_pin",
        "boundary_cut",
        "whole_frame_cut",
        "partial_retained_tail",
        "append_partial",
    ] {
        let (_dir, paths, authority, _) = retention_recovery_fixture();
        erase_retention_frames(&paths.source_log, &authority);
        std::fs::remove_file(pe_service::source_checkpoint::checkpoint_path(
            &paths.source_log,
        ))
        .unwrap();
        let retained_index = matches!(fault, "dependency" | "activation_pin")
            .then(|| SourceReceiptIndex::replay(&paths.source_log).unwrap());
        match fault {
            "authority" => {
                std::fs::remove_file(pe_event_log::retention_path(&paths.source_log)).unwrap()
            }
            "receipts" => {
                std::fs::remove_file(checkpoint_sidecar(&paths.source_log, ".receipts")).unwrap()
            }
            "receipt_checksum" => {
                let file = checkpoint_sidecar(&paths.source_log, ".receipts");
                let mut bytes = std::fs::read(&file).unwrap();
                bytes[0] ^= 1;
                std::fs::write(file, bytes).unwrap();
            }
            "dependency" | "activation_pin" => {
                use std::io::{Seek as _, SeekFrom};
                let pin = authority
                    .pins
                    .iter()
                    .find(|pin| {
                        !pin.reducer
                            && ((pin.sequence == recorded_source(&paths).last_sequence.unwrap())
                                == (fault == "activation_pin"))
                    })
                    .unwrap();
                let mut file = std::fs::OpenOptions::new()
                    .write(true)
                    .open(&paths.source_log)
                    .unwrap();
                file.seek(SeekFrom::Start(pin.offset + 8)).unwrap();
                file.write_all(&[0xff; 16]).unwrap();
            }
            "boundary_cut" | "whole_frame_cut" | "partial_retained_tail" => {
                let cut = match fault {
                    "boundary_cut" => authority.boundary.offset,
                    "partial_retained_tail" => authority.retained_tail.physical_tail - 3,
                    _ => {
                        let mut receipts =
                            std::fs::File::open(checkpoint_sidecar(&paths.source_log, ".receipts"))
                                .unwrap();
                        let mut offset = 0;
                        for seq in 0..=authority.retained_tail.last_sequence.unwrap().0 {
                            offset =
                                pe_event_log::ReceiptRecord::read(&mut receipts, EventSeq(seq))
                                    .unwrap()
                                    .byte_offset
                                    .unwrap();
                        }
                        offset
                    }
                };
                std::fs::OpenOptions::new()
                    .write(true)
                    .open(&paths.source_log)
                    .unwrap()
                    .set_len(cut)
                    .unwrap();
            }
            "append_partial" => {
                std::fs::OpenOptions::new()
                    .append(true)
                    .open(&paths.source_log)
                    .unwrap()
                    .write_all(&[1, 2])
                    .unwrap();
            }
            _ => unreachable!(),
        }
        if let Some(index) = retained_index {
            assert!(index.verify_retention_pins().is_err(), "{fault}");
        }
        let before = std::fs::read(&paths.source_log).unwrap();
        if fault == "append_partial" {
            let opened = SourceLogBoot::open(&paths, true).unwrap().unwrap();
            assert_eq!(
                opened.binding.physical_tail,
                authority.retained_tail.physical_tail
            );
            assert_eq!(
                file_len(&paths.source_log),
                authority.retained_tail.physical_tail
            );
        } else {
            assert!(SourceLogBoot::open(&paths, true).is_err(), "{fault}");
            assert_eq!(
                std::fs::read(&paths.source_log).unwrap(),
                before,
                "{fault}: no truncation"
            );
            assert!(
                SourceReceiptIndex::replay(&paths.source_log).is_err(),
                "{fault}"
            );
        }
    }
}

#[tokio::test]
async fn retention_qualification_returns_retired_before_selecting_pre_start_duplicate() {
    use pe_service::qualification::{QualificationError, QualifyOptions};
    let (_dir, paths) = installed_fixture();
    let payload = serde_json::to_vec(&serde_json::json!([{
        "proxyWallet": WALLET, "timestamp": NOW_UNIX, "conditionId": "condition", "type": "TRADE",
        "size": "2", "usdcSize": "1", "transactionHash": "pre-start", "price": "0.5", "asset": "123", "side": "BUY", "outcomeIndex": 0,
    }])).unwrap();
    append(
        &paths.source_log,
        envelope(
            pe_service::trade_poller::ACTIVITY_POLL_SOURCE_ID,
            ACTIVITY_SCHEMA_VERSION,
            ACTIVITY_PARSER_VERSION,
            &payload,
            NOW_UNIX,
        ),
    );
    append_paper_record(&paths.paper_log, &start_record());
    append(
        &paths.source_log,
        envelope(
            pe_service::trade_poller::ACTIVITY_POLL_SOURCE_ID,
            ACTIVITY_SCHEMA_VERSION,
            ACTIVITY_PARSER_VERSION,
            &payload,
            NOW_UNIX + 1,
        ),
    );
    let boundary = append(
        &paths.source_log,
        envelope("suffix", 1, 1, b"{}", NOW_UNIX + 2),
    );
    retention_capture(&paths.source_log, boundary.sequence.0, &[]);
    let output = paths.source_log.with_extension("report.json");
    std::fs::write(&output, "previous report").unwrap();
    let error = pe_service::qualification::run_qualify(&QualifyOptions {
        paper_log: paths.paper_log,
        source_log: paths.source_log,
        live_journal: Some(paths.live_journal),
        paper_state: paths.fixed_main,
        seal_hash: "invalid-selection-must-not-run".to_owned(),
        output: output.clone(),
    })
    .await
    .unwrap_err();
    assert!(matches!(
        error,
        QualificationError::EventLog(pe_event_log::LogError::Retired { .. })
    ));
    assert_eq!(std::fs::read_to_string(output).unwrap(), "previous report");
}

struct RetentionCompletedAuthority;

impl pe_service::supabase_state::SupabaseStateTrait for RetentionCompletedAuthority {
    async fn commit_fill_v2(
        &self,
        _: &pe_service::supabase_sink::SupabaseFillRow,
    ) -> Result<
        pe_service::supabase_state::FillV2Outcome,
        pe_service::supabase_state::SupabaseStateError,
    > {
        panic!("completed pairs must never call an authority RPC")
    }
    async fn commit_prepared_fill(
        &self,
        _: &PreparedFillRequest,
    ) -> Result<CanonicalFillResult, pe_service::supabase_state::SupabaseStateError> {
        panic!("completed pairs must never call an authority RPC")
    }
    async fn apply_prepared_resolution(
        &self,
        _: &pe_service::supabase_state::PreparedResolutionRequest,
    ) -> Result<
        pe_service::paper_recovery::CanonicalResolutionResult,
        pe_service::supabase_state::SupabaseStateError,
    > {
        panic!("completed pairs must never call an authority RPC")
    }
}

#[tokio::test]
async fn retention_completed_pairs_reproject_old_frame_admission_and_frontier_with_or_without_checkpoint()
 {
    use std::sync::atomic::AtomicU64;
    let logs = support::RetentionLogs::default();
    let _subscriber = tracing::subscriber::set_default(logs.subscriber());
    for checkpoint in [true, false] {
        let (_dir, paths) = installed_fixture();
        let start = append_paper_record(&paths.paper_log, &start_record());
        seal_retention_start(&paths, start);
        let paper = Arc::new(PaperStateDb::open(&paths.fixed_main).unwrap());
        paper
            .reset_financial_era(
                start,
                CollateralAmount::from_decimal_exact(dec!(10)).unwrap(),
            )
            .unwrap();
        install_mixed_current_open_continuations(&paper, &paths.source_log, start);
        let row = paper
            .decision_pending_history()
            .unwrap()
            .into_iter()
            .find(|row| row.wallet == WalletAddress([0xce; 20]))
            .unwrap();
        let wire: serde_json::Value = serde_json::from_str(&row.frozen_inputs_json).unwrap();
        let proof: pe_service::frame_admission::FrameDecisionProof =
            serde_json::from_value(wire["decision_inputs"].clone()).unwrap();
        for pending in paper.open_decision_pending().unwrap() {
            if pending.wallet != row.wallet {
                close_retention_decision(&paper, &pending, NOW_UNIX + 25_010, None);
            }
        }
        let ordinary = append(
            &paths.source_log,
            envelope("old-fill-observation", 1, 1, b"{}", NOW_UNIX + 25_003),
        );
        let mut previous = None;
        let mut latest = None;
        for (receipt, wallet, source_id) in [
            (
                ordinary,
                WalletAddress([0xaa; 20]),
                SourceTradeId("ordinary-completed".to_owned()),
            ),
            (
                proof.inputs.frame_receipt,
                row.wallet,
                row.source_trade_id.clone(),
            ),
        ] {
            let mut economic = support::economic_prepared(receipt, start);
            economic.admission.receipts = pe_execution_core::AdmissionReceipts {
                gamma: ordinary,
                clob_long: ordinary,
                clob_compact: ordinary,
            };
            economic.book_receipt = ordinary;
            economic.observation.as_mut().unwrap().provenance = if wallet == row.wallet {
                "activity_ws"
            } else {
                "reconciled_rest"
            }
            .to_owned();
            let prepared = append_paper_record(
                &paths.paper_log,
                &PaperLogRecord::FinancialPrepared {
                    expected_authority: ExpectedAuthority {
                        qualification_start_receipt: start,
                        prior_completed_prepared_sequence: previous,
                    },
                    payload: FinancialPayload::Fill {
                        operation: PaperFillOperationIdentity {
                            leader_wallet: wallet,
                            source_trade_id: source_id,
                            observed_at_bucket: NOW_UNIX + 25_000,
                        },
                        economic,
                    },
                },
            );
            let final_receipt = append_paper_record(
                &paths.paper_log,
                &PaperLogRecord::FinancialFinal {
                    prepared_receipt: prepared,
                    result: FinancialResult::Fill {
                        canonical: CanonicalFillResult {
                            outcome: "applied".to_owned(),
                            bankroll: if previous.is_none() { dec!(9) } else { dec!(8) },
                            applied_prepared_seq: prepared.sequence,
                            quantity: pe_core_types::ShareAmount::from_whole(2).unwrap(),
                            principal: CollateralAmount::from_decimal_exact(dec!(1)).unwrap(),
                            fee: CollateralAmount::ZERO,
                            fill_price: pe_core_types::Price::new(dec!(0.5)).unwrap(),
                        },
                    },
                },
            );
            if wallet == row.wallet {
                close_retention_decision(&paper, &row, NOW_UNIX + 25_010, Some(final_receipt));
            }
            previous = Some(prepared.sequence);
            latest = Some(prepared.sequence);
        }
        assert_eq!(paper.financial_last_prepared_seq().unwrap(), None);
        assert!(paper.open_decision_pending().unwrap().is_empty());
        append(
            &paths.source_log,
            envelope("old-padding", 1, 1, &retention_padding(), NOW_UNIX + 25_004),
        );
        let recent = append(
            &paths.source_log,
            envelope("recent", 1, 1, b"{}", NOW_UNIX + 8 * 86_400),
        );
        let activation = recorded_source(&paths);
        let mut expected_pins = vec![
            AppendReceipt {
                sequence: activation.last_sequence.unwrap(),
                this_hash: activation.last_hash,
            },
            ordinary,
            proof.inputs.frame_receipt,
            proof.admission_receipt,
            proof.inputs.frontier.commitment,
        ];
        expected_pins.extend(
            proof
                .inputs
                .frontier
                .page_occurrences
                .iter()
                .map(|page| page.receipt),
        );
        expected_pins.sort_by_key(|receipt| receipt.sequence);
        expected_pins.dedup();
        pe_service::source_checkpoint::install_retention_fence(&paths.source_log).unwrap();
        let clock = Arc::new(AtomicU64::new(
            u64::try_from(NOW_UNIX + 8 * 86_400).unwrap(),
        ));
        let (mut owner, control, obligations) = retention_boot_runtime(&paths, &paper, clock);
        assert!(obligations.is_empty());
        owner.initialize_for_scenario().await.unwrap();
        owner.retention_for_scenario().await.unwrap();
        assert!(
            pe_event_log::RetentionAuthority::load(&paths.source_log)
                .unwrap()
                .is_some(),
            "daily job: {}",
            logs.text()
        );
        let authority = pe_event_log::RetentionAuthority::load(&paths.source_log)
            .unwrap()
            .unwrap();
        assert_eq!(authority.epoch, 1);
        assert_eq!(authority.boundary.sequence, recent.sequence);
        assert_eq!(
            authority
                .pins
                .iter()
                .map(|pin| AppendReceipt {
                    sequence: pin.sequence,
                    this_hash: pin.hash,
                })
                .collect::<Vec<_>>(),
            expected_pins,
            "the daily job must select the completed fill's old decision closure"
        );
        assert!(authority.pins.iter().all(|pin| !pin.reducer));
        assert!(authority.advanced_at - row.updated_at_unix > 7 * 86_400);
        assert_eq!(
            checkpoint_json(&paths.source_log)["retention_epoch"],
            authority.epoch
        );
        assert_eq!(checkpoint_json(&paths.source_log)["format_version"], 3);
        assert!(
            std::fs::read(&paths.source_log)
                .unwrap()
                .chunks_exact(4096)
                .any(|block| block.iter().all(|byte| *byte == 0)),
            "the job must finish punching the old padding"
        );
        drop(owner);
        control.abort();
        let _ = control.await;
        if !checkpoint {
            std::fs::remove_file(pe_service::source_checkpoint::checkpoint_path(
                &paths.source_log,
            ))
            .unwrap();
        }
        let opened = SourceLogBoot::open(&paths, true).unwrap().unwrap();
        assert_eq!(opened.boot.checkpoint_assisted_for_scenario(), checkpoint);
        if checkpoint {
            assert_eq!(
                checkpoint_json(&paths.source_log)["retention_epoch"],
                authority.epoch
            );
            assert_eq!(checkpoint_json(&paths.source_log)["format_version"], 3);
        }
        let index = opened.boot.receipt_index();
        drop(opened);
        let before_decision = paper
            .decision_pending_for(&row.source_trade_id)
            .unwrap()
            .unwrap();
        let paper_log = pe_service::paper_recovery::PaperLog::open(&paths.paper_log).unwrap();
        assert_eq!(
            pe_service::supabase_state::reconcile_active_financial_frames(
                &RetentionCompletedAuthority,
                &paper,
                if checkpoint {
                    pe_service::supabase_state::SourceEvidence::Index(&index)
                } else {
                    pe_service::supabase_state::SourceEvidence::Log(&paths.source_log)
                },
                &paper_log,
            )
            .await
            .unwrap(),
            0
        );
        let era = paper_era(scan_paper_log(&paths.paper_log).unwrap());
        let received = pe_service::risk_inputs::completed_prepared_before_boundary(
            &era,
            &paths.source_log,
            NOW_UNIX + 8 * 86_400,
            recent,
        )
        .unwrap();
        assert_eq!(received.len(), 2);
        assert_eq!(
            received,
            pe_service::risk_inputs::completed_prepared_before_boundary_indexed(
                &era,
                &index,
                NOW_UNIX + 8 * 86_400,
                recent
            )
            .unwrap()
        );
        assert_eq!(paper.fills_count().unwrap(), 2);
        assert_eq!(paper.bankroll().unwrap(), Some(dec!(8)));
        assert_eq!(paper.financial_last_prepared_seq().unwrap(), latest);
        assert_eq!(
            paper
                .decision_pending_for(&row.source_trade_id)
                .unwrap()
                .unwrap(),
            before_decision
        );
        assert_eq!(
            index
                .source_envelope_with_pause(proof.admission_receipt, &mut || {})
                .unwrap()
                .this_hash,
            proof.admission_receipt.this_hash
        );
        assert_eq!(
            index
                .source_envelope_with_pause(ordinary, &mut || {})
                .unwrap()
                .this_hash,
            ordinary.this_hash
        );
    }
}

#[test]
fn retention_preparation_retries_advance_after_checkpoint_loss() {
    use std::sync::atomic::{AtomicBool, Ordering};
    for remove_checkpoint in [false, true] {
        let (_dir, paths) = installed_fixture();
        append(&paths.source_log, envelope("old", 1, 1, b"{}", NOW_UNIX));
        let boundary = append(
            &paths.source_log,
            envelope("suffix", 1, 1, b"{}", NOW_UNIX + 1),
        );
        SourceLogBoot::prepare_checkpoint(&paths.fixed_main).unwrap();
        let once = Arc::new(AtomicBool::new(false));
        let source = paths.source_log.clone();
        let activation = recorded_source(&paths);
        let hooks = pe_service::source_checkpoint::PreparationHooks {
            before_cached_hash: Some(Arc::new(move || {
                if !once.swap(true, Ordering::SeqCst) {
                    let authority = retention_capture(
                        &source,
                        boundary.sequence.0,
                        &[(
                            AppendReceipt {
                                sequence: activation.last_sequence.unwrap(),
                                this_hash: activation.last_hash,
                            },
                            false,
                        )],
                    );
                    erase_retention_frames(&source, &authority);
                    if remove_checkpoint {
                        std::fs::remove_file(pe_service::source_checkpoint::checkpoint_path(
                            &source,
                        ))
                        .unwrap();
                    }
                }
                Ok(())
            })),
            ..Default::default()
        };
        let original = pe_service::source_checkpoint::read_authority(&paths.source_log).unwrap();
        SourceLogBoot::prepare_checkpoint_with_hooks(&paths.fixed_main, &hooks).unwrap();
        assert_eq!(
            pe_service::source_checkpoint::read_authority(&paths.source_log).unwrap(),
            original
        );
        assert_eq!(checkpoint_json(&paths.source_log)["format_version"], 3);
        assert_eq!(checkpoint_json(&paths.source_log)["retention_epoch"], 1);
    }
}

#[test]
fn retention_moved_capture_rebinds_and_rebuilds_after_checkpoint_loss() {
    let (_origin, paths, authority, _) = retention_recovery_fixture();
    let paper = PaperStateDb::open(&paths.fixed_main).unwrap();
    let expected = rebuild_reconciliation_obligations(&paths.source_log, &paper).unwrap();
    paper.sync_checkpoint_dispositions().unwrap();
    drop(paper);
    erase_retention_frames(&paths.source_log, &authority);
    let moved = tempfile::tempdir().unwrap();
    let configured = paths_in(moved.path());
    for (from, to) in [
        (&paths.fixed_main, &configured.fixed_main),
        (&paths.source_log, &configured.source_log),
        (&paths.paper_log, &configured.paper_log),
        (&paths.live_journal, &configured.live_journal),
        (
            &pe_event_log::retention_path(&paths.source_log),
            &pe_event_log::retention_path(&configured.source_log),
        ),
        (
            &checkpoint_sidecar(&paths.source_log, ".receipts"),
            &checkpoint_sidecar(&configured.source_log, ".receipts"),
        ),
    ] {
        std::fs::copy(from, to).unwrap();
    }
    let bytes = std::fs::read(&configured.source_log).unwrap();
    assert!(pe_service::paper_migration::update_installed_log_paths(&configured).unwrap());
    assert_eq!(std::fs::read(&configured.source_log).unwrap(), bytes);
    assert!(!pe_service::source_checkpoint::checkpoint_path(&configured.source_log).exists());
    let mut opened = SourceLogBoot::open(&configured, true).unwrap().unwrap();
    assert_eq!(
        opened.binding.path,
        std::fs::canonicalize(&configured.source_log).unwrap()
    );
    opened.boot.extend(&mut opened.sink).unwrap();
    let paper = PaperStateDb::open(&configured.fixed_main).unwrap();
    let mut expected = expected;
    pe_service::trade_poller::recover_daily_boundary(
        &configured.source_log,
        &configured.paper_log,
        &mut expected,
    )
    .unwrap();
    assert_eq!(
        opened
            .boot
            .obligations(&paper, &configured.paper_log)
            .unwrap(),
        expected
    );
    opened
        .boot
        .prepare_installed(configured, NOW_UNIX + 20)
        .unwrap();
}

fn retention_padding() -> Vec<u8> {
    let mut seed = 123_u32;
    (0..20_000)
        .map(|_| {
            seed = seed.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            u8::try_from(seed >> 24).unwrap()
        })
        .collect()
}

fn seal_retention_start(paths: &PaperMigrationPaths, start: AppendReceipt) {
    append_paper_record(
        &paths.paper_log,
        &PaperLogRecord::QualificationSealed(Box::new(
            pe_service::paper_recovery::QualificationSealed {
                start_receipt: start,
                source_prefix: empty_tail(),
                financial_prefix: empty_tail(),
                live_prefix: empty_tail(),
                decision_evidence_digest: "fixture".to_owned(),
                sealed_cutoff_unix: NOW_UNIX,
                reason: pe_service::paper_recovery::SealReason::InsufficientEvidence(
                    "fixture".to_owned(),
                ),
            },
        )),
    );
}

fn close_retention_decision(
    state: &PaperStateDb,
    row: &pe_paper_state::DecisionPendingRow,
    at: i64,
    final_receipt: Option<AppendReceipt>,
) {
    use pe_service::decision_replay::{
        AuthorityEvidence, DecisionClockEvidence, DecisionPostBoundaryEvidence,
        DecisionPostBoundaryEvidenceBody, TerminalDispositionEvidence,
    };
    let continuation =
        pe_service::bucket_commit::DecisionContinuationV3::from_durable(row).unwrap();
    let terminal = final_receipt.map_or_else(
        || TerminalDispositionEvidence {
            disposition: "no_fill".to_owned(),
            reason: "fixture".to_owned(),
            fill: None,
            dispatch_id: None,
            dispatch_control_journal_seq: None,
            decline: None,
            final_receipt: None,
        },
        TerminalDispositionEvidence::final_fill,
    );
    let evidence = DecisionPostBoundaryEvidence::from_body_for_continuation(
        DecisionPostBoundaryEvidenceBody {
            version: if final_receipt.is_some() { 5 } else { 4 },
            owners: vec!["source_log".to_owned(), "paper_log".to_owned()],
            source_trade_id: row.source_trade_id.clone(),
            applied_configuration_hash: continuation.facts.applied_configuration_hash.clone(),
            market_end: None,
            market_price: None,
            book: None,
            clocks: if final_receipt.is_some() {
                vec![
                    DecisionClockEvidence::precise(
                        "paper_prepared_staleness_gate",
                        i128::from(at) * 1_000_000_000,
                    )
                    .unwrap(),
                ]
            } else {
                Vec::new()
            },
            authority: AuthorityEvidence {
                kind: if final_receipt.is_some() {
                    "commit_fill_v2"
                } else {
                    "not_read"
                }
                .to_owned(),
                outcome: if final_receipt.is_some() {
                    "applied"
                } else {
                    "terminal_before_fill_authority"
                }
                .to_owned(),
                bankroll: None,
            },
            terminal,
        },
        &continuation,
    )
    .unwrap();
    state
        .close_decision_pending(
            &row.source_trade_id,
            &serde_json::to_string(&evidence).unwrap(),
            &evidence.body.terminal.disposition,
            at,
        )
        .unwrap();
    pe_service::decision_replay::replay_decision_pending(
        &state
            .decision_pending_for(&row.source_trade_id)
            .unwrap()
            .unwrap(),
    )
    .unwrap();
}

fn retention_boot_runtime(
    paths: &PaperMigrationPaths,
    state: &Arc<PaperStateDb>,
    clock: Arc<std::sync::atomic::AtomicU64>,
) -> (
    pe_service::source_checkpoint::SourceCheckpointOwner,
    tokio::task::JoinHandle<()>,
    pe_service::trade_poller::ReconciliationObligations,
) {
    let (owner, task, obligations, _, _) = retention_boot_runtime_with_control(paths, state, clock);
    (owner, task, obligations)
}

fn retention_boot_runtime_with_control(
    paths: &PaperMigrationPaths,
    state: &Arc<PaperStateDb>,
    clock: Arc<std::sync::atomic::AtomicU64>,
) -> (
    pe_service::source_checkpoint::SourceCheckpointOwner,
    tokio::task::JoinHandle<()>,
    pe_service::trade_poller::ReconciliationObligations,
    tokio::sync::mpsc::Sender<pe_service::orchestrator_control::OrchestratorControl>,
    Arc<pe_service::orchestrator::ScenarioHooks>,
) {
    let mut opened = SourceLogBoot::open(paths, true).unwrap().unwrap();
    opened.boot.extend(&mut opened.sink).unwrap();
    let obligations = opened.boot.obligations(state, &paths.paper_log).unwrap();
    let boot_wallets = obligations.wallets().collect();
    let (_, held) = tokio::sync::watch::channel(pe_service::trade_poller::HeldObligations::from(
        &obligations,
    ));
    let index = opened.boot.receipt_index();
    let paper = pe_service::paper_recovery::PaperLog::open(&paths.paper_log).unwrap();
    let terminal_at = OffsetDateTime::from_unix_timestamp(
        i64::try_from(clock.load(std::sync::atomic::Ordering::Acquire)).unwrap(),
    )
    .unwrap();
    let hooks = Arc::new(pe_service::orchestrator::ScenarioHooks::default());
    let (live, preparer, control, task) = support::retention_controls_at(
        paper.clone(),
        state.clone(),
        index,
        Some(terminal_at),
        Some(hooks.clone()),
    );
    let database = pe_service::database_retention::DatabaseRetention::new(
        state.clone(),
        paper.clone(),
        live,
        preparer,
        control.clone(),
        boot_wallets,
    )
    .unwrap();
    let db_state = state.clone();
    let context = pe_service::source_checkpoint::RetentionContext {
        paper_state: state.clone(),
        paper_log: Arc::new(paper),
        live_journal_path: None,
        control: control.clone(),
        database_inputs: Arc::new(move || {
            pe_service::source_checkpoint::RetentionDatabaseInputs::read(&db_state)
        }),
        hooks: Arc::new(Default::default()),
    };
    let mut owner = opened
        .boot
        .into_checkpoint_owner(Default::default())
        .with_database_retention(database)
        .with_held_obligations(held)
        .with_retention(context)
        .unwrap();
    owner.set_scenario_hooks(Arc::new(
        pe_service::source_checkpoint::CheckpointOwnerHooks {
            clock: Some(Arc::new(move || {
                Ok(clock.load(std::sync::atomic::Ordering::Acquire) * 1000)
            })),
            ..Default::default()
        },
    ));
    drop(opened.sink);
    (owner, task, obligations, control, hooks)
}

fn record_failed_catch_up_group(
    state: &PaperStateDb,
    observation: &pe_source_polymarket_public::ActivityTradeObservation,
) {
    state
        .commit_activity_bucket(&pe_paper_state::ActivityBucketCommit {
            wallet: observation.wallet,
            source_epoch: observation.source_time.0.unix_timestamp(),
            dispositions: vec![pe_paper_state::ActivityDispositionRecord {
                source_trade_id: observation.group_id.key().clone(),
                transaction_hash: "0xdeparted".to_owned(),
                wallet: observation.wallet,
                source_epoch: observation.source_time.0.unix_timestamp(),
                semantic_revision: "fixture".to_owned(),
                activity_type: "TRADE".to_owned(),
                disposition: "ledger_only".to_owned(),
                proof_json: "{}".to_owned(),
                no_copy: None,
            }],
            leader_positions: Vec::new(),
            gate_results: Vec::new(),
            history_effects: vec![pe_paper_state::MarketHistoryRecord {
                wallet: observation.wallet,
                market_id: MarketId(pe_core_types::VenueMarketId(format!(
                    "0x{}",
                    "11".repeat(32)
                ))),
                first_epoch: observation.source_time.0.unix_timestamp(),
                source_trade_id: observation.group_id.key().clone(),
            }],
            history_status: None,
            pending: Vec::new(),
            fence: None,
            reanchor: None,
            advance_cursor: false,
        })
        .unwrap();
}

#[tokio::test]
async fn retention_terminalized_during_boot_waits_through_two_advances_and_checkpoint_loss() {
    use pe_service::database_retention::RETENTION_BUFFER_SECS;
    use std::sync::atomic::{AtomicU64, Ordering};

    let (_dir, paths) = installed_fixture();
    let start = append_paper_record(&paths.paper_log, &start_record());
    seal_retention_start(&paths, start);
    let state = Arc::new(PaperStateDb::open(&paths.fixed_main).unwrap());
    install_mixed_current_open_continuations(&state, &paths.source_log, start);
    let wallet = WalletAddress([0xcc; 20]);
    let pending = state
        .open_decision_pending()
        .unwrap()
        .into_iter()
        .find(|row| row.wallet == wallet)
        .unwrap();
    for row in state
        .open_decision_pending()
        .unwrap()
        .into_iter()
        .filter(|row| row.wallet != wallet)
    {
        close_retention_decision(&state, &row, NOW_UNIX + 25_010, None);
    }
    let terminal_at = NOW_UNIX + 8 * 86_400;
    assert!(terminal_at - pending.updated_at_unix > RETENTION_BUFFER_SECS);
    for at in [
        terminal_at - RETENTION_BUFFER_SECS + 100,
        terminal_at - RETENTION_BUFFER_SECS + 86_400 + 100,
        terminal_at,
    ] {
        append(&paths.source_log, envelope("clock-only", 1, 1, b"{}", at));
    }
    let source_before = std::fs::read(&paths.source_log).unwrap();
    pe_service::source_checkpoint::install_retention_fence(&paths.source_log).unwrap();
    let clock = Arc::new(AtomicU64::new(u64::try_from(terminal_at).unwrap()));
    let logs = support::RetentionLogs::default();
    let _subscriber = tracing::subscriber::set_default(logs.subscriber());
    let (mut owner, control, obligations) = retention_boot_runtime(&paths, &state, clock.clone());
    assert!(obligations.is_empty());
    owner.initialize_for_scenario().await.unwrap();
    let terminal = state
        .decision_pending_for(&pending.source_trade_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        terminal.state,
        pe_paper_state::DecisionPendingState::Terminal
    );
    assert_eq!(terminal.updated_at_unix, terminal_at);
    assert_eq!(
        std::fs::read(&paths.source_log).unwrap(),
        source_before,
        "terminalization must append no feed message or read commitment"
    );
    let history = state
        .market_history_record(
            &wallet,
            &MarketId(VenueMarketId(format!("0x{:064x}", 0xcc))),
        )
        .unwrap()
        .unwrap();
    for (epoch, at) in [(1, terminal_at), (2, terminal_at + 86_400)] {
        clock.store(u64::try_from(at).unwrap(), Ordering::Release);
        owner.retention_for_scenario().await.unwrap();
        assert_eq!(
            pe_event_log::RetentionAuthority::load(&paths.source_log)
                .unwrap()
                .unwrap()
                .epoch,
            epoch
        );
        assert_eq!(checkpoint_json(&paths.source_log)["retention_epoch"], epoch);
        assert_eq!(checkpoint_json(&paths.source_log)["format_version"], 3);
        assert_eq!(
            logs.last_run()["wallets_waiting"],
            format!("[({wallet:?}, Durable(RecentDecision))]")
        );
        assert_eq!(
            state
                .decision_pending_for(&pending.source_trade_id)
                .unwrap(),
            Some(terminal.clone())
        );
        assert!(
            state
                .activity_group_state(&pending.source_trade_id)
                .unwrap()
                .is_some()
        );
    }
    drop(owner);
    control.abort();
    let _ = control.await;
    std::fs::remove_file(pe_service::source_checkpoint::checkpoint_path(
        &paths.source_log,
    ))
    .unwrap();
    let (mut owner, control, obligations) = retention_boot_runtime(&paths, &state, clock.clone());
    assert!(obligations.is_empty());
    owner.initialize_for_scenario().await.unwrap();
    assert_eq!(
        logs.last_run()["wallets_waiting"],
        format!("[({wallet:?}, Durable(RecentDecision))]")
    );
    clock.store(
        u64::try_from(terminal_at + RETENTION_BUFFER_SECS).unwrap(),
        Ordering::Release,
    );
    owner.retention_for_scenario().await.unwrap();
    assert_eq!(
        logs.last_run()["wallets_waiting"],
        format!("[({wallet:?}, Durable(RecentDecision))]")
    );
    clock.store(
        u64::try_from(terminal_at + RETENTION_BUFFER_SECS + 86_400).unwrap(),
        Ordering::Release,
    );
    owner.retention_for_scenario().await.unwrap();
    assert_eq!(logs.last_run()["wallets_swapped_out"], 1);
    assert_eq!(logs.last_run()["wallets_waiting"], "[]");
    assert!(
        state
            .activity_group_state(&pending.source_trade_id)
            .unwrap()
            .is_none()
    );
    assert!(
        state
            .decision_pending_for(&pending.source_trade_id)
            .unwrap()
            .is_none()
    );
    assert!(!state.is_seen(&pending.source_trade_id).unwrap());
    assert!(
        state
            .no_copy_disposition(&pending.source_trade_id)
            .unwrap()
            .is_none()
    );
    assert!(
        state
            .activity_revision_state(&pending.source_trade_id, &pending.semantic_revision)
            .unwrap()
            .is_none()
    );
    assert!(
        state
            .entry_gate_result(&pending.source_trade_id)
            .unwrap()
            .is_none()
    );
    assert!(!state.wallet_history_complete(&wallet).unwrap());
    assert!(state.position_anchors(&wallet).unwrap().is_empty());
    assert!(state.position_validation(&wallet).unwrap().is_none());
    assert!(state.cursor(&wallet).unwrap().is_none());
    assert!(
        state
            .leader_positions()
            .unwrap()
            .iter()
            .all(|row| row.wallet != wallet)
    );
    assert_eq!(
        state
            .market_history_record(&wallet, &history.market_id)
            .unwrap(),
        Some(history)
    );
    drop(owner);
    control.abort();
    let _ = control.await;
}

#[tokio::test]
async fn retention_returning_wallet_catches_up_in_process_after_reboot_and_checkpoint_loss() {
    use pe_copy_signal_engine::{LeaderSignal, PositionState};
    use pe_core_types::{
        LeaderAction, MarketOutcomeId, Price, ProbabilityPpm, ShareAmount, Side, TraderId, VenueId,
    };
    use pe_service::asset_identity::AssetIdentityResolver;
    use pe_service::entry_gate::{CopyEntryGate, CopyEntryGateConfig, GateReject};
    use pe_service::orchestrator_control::OrchestratorControl;
    use pe_service::position_seeder::{
        CausalPositionValidator, anchor_proves_full_history, ledger_capture,
    };
    use pe_service::source_event_sink::SourceEventSink;
    use pe_service::watchlist_admission::AdmissionPreparer;
    use pe_source_polymarket_public::{
        FixtureFetcher, GAMMA_BATCH_SIZE, PolymarketEndpoint, PositionPartition,
        ReconciliationFetcher,
    };
    use std::collections::HashMap;
    use std::sync::atomic::AtomicU64;
    use tokio::sync::{Mutex, oneshot};

    for mode in ["same_process", "reboot", "checkpoint_loss"] {
        let (_dir, paths) = installed_fixture();
        let start = append_paper_record(&paths.paper_log, &start_record());
        seal_retention_start(&paths, start);
        let payload = activity_payload("0xdeparted", NOW_UNIX + 1);
        let observation =
            pe_source_polymarket_public::parse_activity_trade_observation(&payload).unwrap();
        let feed = append(
            &paths.source_log,
            activity_envelope("0xdeparted", NOW_UNIX + 1),
        );
        let now = NOW_UNIX + 8 * 86_400;
        append(&paths.source_log, envelope("recent", 1, 1, b"{}", now));
        let mut state = Arc::new(PaperStateDb::open(&paths.fixed_main).unwrap());
        record_failed_catch_up_group(&state, &observation);
        let old_market = MarketId(VenueMarketId(format!("0x{}", "11".repeat(32))));
        let history = state
            .market_history_record(&observation.wallet, &old_market)
            .unwrap()
            .unwrap();
        Connection::open(&paths.fixed_main)
            .unwrap()
            .execute(
                "INSERT INTO leader_positions VALUES (?1, ?2, 0, '10', '0')",
                rusqlite::params![observation.wallet.to_string(), old_market.0.0],
            )
            .unwrap();
        pe_service::source_checkpoint::install_retention_fence(&paths.source_log).unwrap();
        let clock = Arc::new(AtomicU64::new(u64::try_from(now).unwrap()));
        let (mut owner, mut task, obligations, mut tx, mut hooks) =
            retention_boot_runtime_with_control(&paths, &state, clock.clone());
        assert!(obligations.is_empty());
        owner.initialize_for_scenario().await.unwrap();
        owner.retention_for_scenario().await.unwrap();
        let authority = pe_event_log::RetentionAuthority::load(&paths.source_log)
            .unwrap()
            .unwrap();
        assert_eq!(authority.epoch, 1);
        assert!(authority.pin(feed.sequence).is_none());
        assert!(
            state
                .activity_group_state(observation.group_id.key())
                .unwrap()
                .is_none()
        );
        assert!(!state.wallet_history_complete(&observation.wallet).unwrap());
        assert!(
            state
                .leader_positions()
                .unwrap()
                .iter()
                .all(|row| row.wallet != observation.wallet)
        );
        assert_eq!(
            state
                .market_history_record(&observation.wallet, &old_market)
                .unwrap(),
            Some(history.clone())
        );
        if mode != "same_process" {
            drop(owner);
            task.abort();
            let _ = task.await;
            if mode == "checkpoint_loss" {
                std::fs::remove_file(pe_service::source_checkpoint::checkpoint_path(
                    &paths.source_log,
                ))
                .unwrap();
            }
            state = Arc::new(PaperStateDb::open(&paths.fixed_main).unwrap());
            let opened = SourceLogBoot::open(&paths, true).unwrap().unwrap();
            assert_eq!(
                opened.boot.checkpoint_assisted_for_scenario(),
                mode == "reboot"
            );
            drop(opened);
            let (restarted, restarted_task, obligations, restarted_tx, restarted_hooks) =
                retention_boot_runtime_with_control(&paths, &state, clock.clone());
            assert!(obligations.is_empty());
            assert!(obligations.frame_recovery_receipts().0.is_empty());
            assert!(obligations.frame_recovery_receipts().1.is_empty());
            owner = restarted;
            task = restarted_task;
            tx = restarted_tx;
            hooks = restarted_hooks;
            owner.initialize_for_scenario().await.unwrap();
        }
        let base = "https://api.example.com";
        let wallet = observation.wallet;
        let new_market = MarketId(VenueMarketId(format!("0x{}", "22".repeat(32))));
        let activity = serde_json::to_vec(&serde_json::json!([{
            "proxyWallet":wallet, "timestamp":NOW_UNIX + 10, "conditionId":new_market.0.0,
            "type":"TRADE", "size":"25", "usdcSize":"12.5", "transactionHash":"0xreturn",
            "price":"0.5", "asset":"456", "side":"BUY", "outcomeIndex":0,
            "outcome":"Yes", "isCombo":false
        }]))
        .unwrap();
        let positions = serde_json::to_vec(&serde_json::json!([{
            "proxyWallet":wallet, "asset":"456", "conditionId":new_market.0.0,
            "size":"25", "outcomeIndex":0, "negativeRisk":false
        }]))
        .unwrap();
        let fetcher: Arc<dyn ReconciliationFetcher> =
            Arc::new(FixtureFetcher::new(HashMap::from([
                (
                    PolymarketEndpoint::UserPositionActivityPage {
                        user: wallet.to_string(),
                        end: now,
                        start: Some(1),
                        offset: 0,
                    }
                    .url(base),
                    activity,
                ),
                (
                    PolymarketEndpoint::CurrentPositionsReconciliationPage {
                        user: wallet.to_string(),
                        partition: PositionPartition::NotRedeemable,
                        offset: 0,
                    }
                    .url(base),
                    positions,
                ),
                (
                    PolymarketEndpoint::CurrentPositionsReconciliationPage {
                        user: wallet.to_string(),
                        partition: PositionPartition::Redeemable,
                        offset: 0,
                    }
                    .url(base),
                    b"[]".to_vec(),
                ),
                (
                    format!("{base}/markets?clob_token_ids=456&limit=500"),
                    serde_json::to_vec(&serde_json::json!([{
                        "conditionId":new_market.0.0, "clobTokenIds":["456"], "closed":false
                    }]))
                    .unwrap(),
                ),
            ])));
        let sink = Arc::new(Mutex::new(
            SourceEventSink::open(&paths.source_log).unwrap(),
        ));
        let resolver = Arc::new(AssetIdentityResolver::new(
            fetcher.clone(),
            base.to_owned(),
            GAMMA_BATCH_SIZE,
            sink.clone(),
        ));
        let validator = CausalPositionValidator::new_recording(
            fetcher,
            base,
            "scenario-return",
            sink.clone(),
            resolver,
        )
        .with_clock(Arc::new(move || now));
        let preparer = AdmissionPreparer::with_validator(tx.clone(), state.clone(), validator);
        let result = preparer.prepare(&[wallet]).await.unwrap();
        assert_eq!(result.admitted, vec![wallet], "{mode}: {result:?}");
        assert!(result.deferred.is_empty());
        assert!(state.wallet_history_complete(&wallet).unwrap());
        assert!(state.position_validation_current(&wallet).unwrap());
        let anchors = state.position_anchors(&wallet).unwrap();
        assert_eq!(anchors.len(), 1);
        assert!(anchor_proves_full_history(&anchors[0].proof_json));
        let mut expected = pe_position_ledger::PositionLedger::new();
        expected.replace_wallet_snapshot(
            wallet,
            HashMap::from([(
                MarketOutcomeId::new(new_market, OutcomeId(0)),
                PositionState {
                    long_contracts: ShareAmount::from_whole(25).unwrap(),
                    short_contracts: ShareAmount::ZERO,
                },
            )]),
        );
        let (captured, capture) = oneshot::channel();
        tx.send(OrchestratorControl::CaptureAdmissionLedger { wallet, captured })
            .await
            .unwrap();
        assert_eq!(
            capture.await.unwrap().unwrap(),
            ledger_capture(&expected, &state, wallet).unwrap(),
            "{mode}"
        );
        assert!(
            hooks.frame_barriers.lock().unwrap()[&wallet].is_empty(),
            "{mode}: runtime restored an old frame barrier"
        );
        assert_eq!(
            ledger_capture(
                &pe_service::paper_recovery::build_leader_ledger(&state).unwrap(),
                &state,
                wallet
            )
            .unwrap(),
            ledger_capture(&expected, &state, wallet).unwrap()
        );
        let signal = LeaderSignal {
            leader: TraderId(wallet),
            venue: VenueId::polymarket(),
            market_id: old_market.clone(),
            outcome_id: OutcomeId(0),
            action: LeaderAction::Entry,
            leader_side: Side::Buy,
            leader_price: Price::new(dec!(0.5)).unwrap(),
            leader_size: ShareAmount::from_whole(1).unwrap(),
            observed_at: OffsetDateTime::from_unix_timestamp(now).unwrap(),
            received_at: OffsetDateTime::from_unix_timestamp(now).unwrap(),
            reconstruction_quality: ReconstructionQuality::new(100).unwrap(),
            source_trade_id: SourceTradeId("return-to-old-market".to_owned()),
            action_confidence_ppm: ProbabilityPpm(1_000_000),
        };
        assert_eq!(
            state.market_history_record(&wallet, &old_market).unwrap(),
            Some(history)
        );
        assert_eq!(
            CopyEntryGate::new(CopyEntryGateConfig, state.gate_history().unwrap()).admit(&signal),
            Some(GateReject::NotFirstEntry),
            "{mode}"
        );
        let rebuilt = rebuild_reconciliation_obligations(&paths.source_log, &state).unwrap();
        assert!(rebuilt.is_empty(), "{mode}");
        assert!(rebuilt.frame_recovery_receipts().0.is_empty(), "{mode}");
        assert!(rebuilt.frame_recovery_receipts().1.is_empty(), "{mode}");
        assert!(
            state
                .activity_group_state(observation.group_id.key())
                .unwrap()
                .is_none()
        );
        drop(preparer);
        drop(sink);
        drop(owner);
        task.abort();
        let _ = task.await;
        let mut opened = SourceLogBoot::open(&paths, true).unwrap().unwrap();
        opened.boot.extend(&mut opened.sink).unwrap();
        let obligations = opened.boot.obligations(&state, &paths.paper_log).unwrap();
        assert!(obligations.is_empty(), "{mode}");
        assert!(obligations.frame_recovery_receipts().0.is_empty(), "{mode}");
        assert!(obligations.frame_recovery_receipts().1.is_empty(), "{mode}");
    }
}

#[tokio::test]
async fn retention_daily_job_swap_out_and_failed_catch_up_never_restore_feed_work() {
    use std::sync::atomic::{AtomicU64, Ordering};
    for failed_catch_up in [false, true] {
        let (_dir, paths) = installed_fixture();
        let start = append_paper_record(&paths.paper_log, &start_record());
        seal_retention_start(&paths, start);
        let feed = append(
            &paths.source_log,
            activity_envelope("0xdeparted", NOW_UNIX + 1),
        );
        let observation = pe_source_polymarket_public::parse_activity_trade_observation(
            &activity_payload("0xdeparted", NOW_UNIX + 1),
        )
        .unwrap();
        let now = NOW_UNIX + 8 * 86_400;
        append(&paths.source_log, envelope("recent", 1, 1, b"{}", now));
        let state = Arc::new(PaperStateDb::open(&paths.fixed_main).unwrap());
        if !failed_catch_up {
            record_failed_catch_up_group(&state, &observation);
        }
        pe_service::source_checkpoint::install_retention_fence(&paths.source_log).unwrap();
        let clock = Arc::new(AtomicU64::new(u64::try_from(now).unwrap()));
        let (mut owner, control, obligations) =
            retention_boot_runtime(&paths, &state, clock.clone());
        assert_eq!(obligations.is_empty(), !failed_catch_up);
        owner.initialize_for_scenario().await.unwrap();
        owner.retention_for_scenario().await.unwrap();
        let first = pe_event_log::RetentionAuthority::load(&paths.source_log)
            .unwrap()
            .unwrap();
        assert_eq!(first.epoch, 1);
        if failed_catch_up {
            assert!(first.pins.iter().any(|pin| pin.sequence == feed.sequence
                && pin.reducer
                && pin.wallet == Some(observation.wallet)));
            record_failed_catch_up_group(&state, &observation);
            drop(owner);
            control.abort();
            let _ = control.await;
            clock.store(u64::try_from(now + 2 * 86_400).unwrap(), Ordering::Release);
            let (mut restarted, control, obligations) =
                retention_boot_runtime(&paths, &state, clock.clone());
            assert!(obligations.is_empty());
            assert!(obligations.frame_recovery_receipts().0.is_empty());
            restarted.initialize_for_scenario().await.unwrap();
            // A complete walk with nothing to advance must keep the suppressed reducer pin.
            restarted.retention_for_scenario().await.unwrap();
            assert!(restarted.retention_window_for_scenario().is_some());
            assert_eq!(
                pe_event_log::RetentionAuthority::load(&paths.source_log)
                    .unwrap()
                    .unwrap()
                    .epoch,
                1
            );
            assert!(
                state
                    .activity_group_state(observation.group_id.key())
                    .unwrap()
                    .is_some()
            );
            // The next publication prunes the reducer; the next daily advance can drop its pin.
            clock.store(u64::try_from(now + 8 * 86_400).unwrap(), Ordering::Release);
            let input = envelope("next-day", 1, 1, b"{}", now + 8 * 86_400);
            let receipt = append(
                &paths.source_log,
                envelope("next-day", 1, 1, b"{}", now + 8 * 86_400),
            );
            restarted
                .record_synced_append_for_scenario(receipt, &input)
                .unwrap();
            restarted.publish_hourly_for_scenario().await.unwrap();
            restarted.retention_for_scenario().await.unwrap();
            let authority = pe_event_log::RetentionAuthority::load(&paths.source_log)
                .unwrap()
                .unwrap();
            assert_eq!(authority.epoch, 2);
            assert!(authority.pin(feed.sequence).is_none());
            drop(restarted);
            control.abort();
            let _ = control.await;
        } else {
            assert!(first.pin(feed.sequence).is_none());
            drop(owner);
            control.abort();
            let _ = control.await;
        }
        assert!(
            state
                .activity_group_state(observation.group_id.key())
                .unwrap()
                .is_none()
        );
        assert!(
            state
                .market_history_record(
                    &observation.wallet,
                    &MarketId(pe_core_types::VenueMarketId(format!(
                        "0x{}",
                        "11".repeat(32)
                    )))
                )
                .unwrap()
                .is_some()
        );
        for checkpoint in [true, false] {
            if !checkpoint {
                std::fs::remove_file(pe_service::source_checkpoint::checkpoint_path(
                    &paths.source_log,
                ))
                .unwrap();
            }
            let (mut restarted, control, obligations) =
                retention_boot_runtime(&paths, &state, clock.clone());
            assert!(obligations.is_empty());
            assert!(obligations.frame_recovery_receipts().0.is_empty());
            assert!(obligations.frame_recovery_receipts().1.is_empty());
            restarted.initialize_for_scenario().await.unwrap();
            let rebuilt = rebuild_reconciliation_obligations(&paths.source_log, &state).unwrap();
            assert!(rebuilt.is_empty());
            assert!(rebuilt.frame_recovery_receipts().0.is_empty());
            assert!(rebuilt.frame_recovery_receipts().1.is_empty());
            drop(restarted);
            control.abort();
            let _ = control.await;
        }
    }
}

#[test]
fn retention_complete_prefix_era_commands_refuse_before_selection() {
    use pe_service::qualification::{FinancialEraCommand, QualificationError};
    let (dir, paths, _, _) = retention_recovery_fixture();
    let manifest = serde_json::json!({
        "kind": "financial-era-v1", "state": "prepared", "activation_id": "fixture", "generation": "fixture",
        "fresh_bankroll": 10_000_000, "target_revision": "fixture", "artifact_blake3": "fixture", "static_config_hash": "fixture",
        "ranking_batch_id": 1, "membership": [], "schema_version": 3, "parser_version": 1,
        "financial_semantic_version": 3, "start_unix": NOW_UNIX,
        "paths": { "paper_log": paths.paper_log, "source_log": paths.source_log,
            "live_journal": paths.live_journal, "paper_state": paths.fixed_main },
        "old_artifact_sha256": "fixture", "target_artifact_sha256": "fixture", "old_config_sha256": "fixture",
        "target_config_sha256": "fixture", "old_environment_sha256": "fixture", "target_environment_sha256": "fixture", "preparation": null,
    });
    let path = dir.path().join("financial-era.json");
    std::fs::write(&path, serde_json::to_vec(&manifest).unwrap()).unwrap();
    for command in [
        FinancialEraCommand::Prepare,
        FinancialEraCommand::Start,
        FinancialEraCommand::RollbackCheck,
    ] {
        let error =
            pe_service::qualification::run_financial_era(command, &path, &Default::default(), None)
                .unwrap_err();
        assert!(
            matches!(
                error,
                QualificationError::EventLog(pe_event_log::LogError::Retired { .. })
            ),
            "{command:?}: {error}"
        );
    }
}

#[tokio::test]
async fn retention_open_continuation_census_and_preparation_authenticate_pinned_closure() {
    let (_dir, paths) = installed_fixture();
    let paper = Arc::new(PaperStateDb::open(&paths.fixed_main).unwrap());
    install_committed_open_read(&paper, &paths.source_log);
    let old: Vec<_> = pe_event_log::Reader::replay(&paths.source_log)
        .unwrap()
        .map(|item| {
            let (sequence, envelope) = item.unwrap();
            (
                AppendReceipt {
                    sequence,
                    this_hash: envelope.this_hash,
                },
                false,
            )
        })
        .collect();
    let padding = append(
        &paths.source_log,
        envelope("old-padding", 1, 1, b"{}", NOW_UNIX + 25_004),
    );
    append(
        &paths.source_log,
        envelope("recent", 1, 1, b"{}", NOW_UNIX + 8 * 86_400),
    );
    let authority = retention_capture(&paths.source_log, padding.sequence.0 + 1, &old);
    erase_retention_frames(&paths.source_log, &authority);
    let index = SourceReceiptIndex::replay(&paths.source_log).unwrap();
    assert_eq!(
        pe_service::bucket_commit::validate_open_continuations(&paper, &index).unwrap(),
        1
    );
    assert_eq!(
        SourceLogBoot::prepare_checkpoint(&paths.fixed_main)
            .unwrap()
            .1,
        1
    );
    let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_pe-service"));
    command
        .env_clear()
        .arg("--validate-open-continuations")
        .arg("--paper-state")
        .arg(&paths.fixed_main)
        .arg("--source-log")
        .arg(&paths.source_log);
    let output = support::bounded_command_output(command).await;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(output.stdout, b"open_rows=1 validated=1\n");
}
