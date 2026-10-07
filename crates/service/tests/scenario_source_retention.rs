//! Daily-retention crash and pause boundaries, using the real checkpoint owner and commit handler.
#![cfg(feature = "scenario")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::duplicate_mod
)]

mod support;

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use pe_core_types::{CollateralAmount, ReceivedAt, SourceId, SourceTimestamp, WalletAddress};
use pe_event_log::{
    AppendReceipt, ContentType, EnvelopeIn, FeedArchiveIter, LogTailBinding, RetentionAuthority,
    Scanner, Writer,
};
use pe_paper_state::PaperStateDb;
use pe_service::paper_recovery::{
    PaperLog, PaperLogRecord, QualificationSealed, QualificationStarted, SealReason, TailBinding,
};
use pe_service::risk_inputs::SourceReceiptIndex;
use pe_service::source_checkpoint::{
    CheckpointJobSlot, CheckpointOwnerHooks, RetentionContext, RetentionDatabaseInputs,
    RetentionHooks, SourceCheckpointOwner, checkpoint_path, install_retention_fence, receipts_path,
};

const NOW: i64 = 1_800_000_000;
const OLD: i64 = NOW - 8 * 24 * 3600;
const WALLET: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const OTHER: &str = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

fn noise(length: usize, seed: u8) -> Vec<u8> {
    let mut payload = Vec::with_capacity(length);
    for block in 0..length.div_ceil(32) {
        let mut hasher = blake3::Hasher::new();
        hasher.update(&[seed]);
        hasher.update(&block.to_le_bytes());
        payload.extend_from_slice(hasher.finalize().as_bytes());
    }
    payload.truncate(length);
    payload
}

fn input(source: &str, payload: Vec<u8>, unix: i64) -> EnvelopeIn {
    let clock = time::OffsetDateTime::from_unix_timestamp(unix).unwrap();
    EnvelopeIn {
        source_id: SourceId(source.to_owned()),
        schema_version: 1,
        parser_version: 1,
        observed_at: SourceTimestamp(clock),
        received_at: ReceivedAt(clock),
        content_type: ContentType::Json,
        payload,
    }
}

fn feed_input(transaction: &str, unix: i64) -> EnvelopeIn {
    let payload = format!(
        r#"{{"proxyWallet":"{WALLET}","conditionId":"0x{}","asset":"123","side":"BUY","size":"100","price":"0.50","timestamp":"{OLD}","transactionHash":"{transaction}","outcomeIndex":"0"}}"#,
        "11".repeat(32)
    );
    let mut envelope = input(
        pe_service::activity_ingest::ACTIVITY_WS_SOURCE_ID,
        payload.into_bytes(),
        unix,
    );
    envelope.schema_version = pe_source_polymarket_public::ACTIVITY_SCHEMA_VERSION;
    envelope.parser_version = pe_source_polymarket_public::ACTIVITY_PARSER_VERSION;
    envelope
}

fn append_paper(log: &PaperLog, record: &PaperLogRecord) -> AppendReceipt {
    let mut envelope = input("pe-service.paper", serde_json::to_vec(record).unwrap(), NOW);
    envelope.schema_version = 2;
    log.append_synced(envelope).unwrap()
}

fn start() -> PaperLogRecord {
    let empty = TailBinding {
        physical_tail: 5,
        last_sequence: None,
        last_hash: "00".repeat(32),
    };
    PaperLogRecord::QualificationStarted(Arc::new(QualificationStarted {
        starting_bankroll: CollateralAmount::from_decimal_exact(rust_decimal::Decimal::from(10))
            .unwrap(),
        paper_prefix: empty.clone(),
        source_prefix: empty.clone(),
        live_prefix: empty,
        artifact_blake3: "artifact".into(),
        static_config_hash: "static".into(),
        hot_config_hash: "hot".into(),
        generation: "retention-fixture".into(),
        activation_id: "retention-fixture".into(),
        ranking_batch_id: 1,
        membership: vec![],
        membership_proofs_hash: "proof".into(),
        schema_version: 3,
        parser_version: 1,
        financial_semantic_version: 3,
    }))
}

fn manifest(path: &Path) -> serde_json::Value {
    let bytes = fs::read(checkpoint_path(path)).unwrap();
    serde_json::from_slice(&bytes[65..]).unwrap()
}

struct Fixture {
    _dir: tempfile::TempDir,
    path: PathBuf,
    activation: LogTailBinding,
    index: SourceReceiptIndex,
    state: Arc<PaperStateDb>,
    seeds: Arc<AtomicBool>,
    context: RetentionContext,
    control: tokio::task::JoinHandle<()>,
    feed: AppendReceipt,
    feed_bytes: Vec<u8>,
    feed_offset: u64,
    feed_end: u64,
    evidence: Vec<AppendReceipt>,
}

impl Fixture {
    fn new(sealed: Option<bool>) -> Self {
        Self::with_evidence(sealed, 0)
    }

    fn with_evidence(sealed: Option<bool>, evidence_count: usize) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source.log");
        let mut writer = Writer::open(&path).unwrap();
        writer
            .append_synced(input("activation", noise(12_000, 1), OLD))
            .unwrap();
        let activation = writer.verified_tail().unwrap();
        writer
            .append_synced(input("old-raw", noise(32_000, 2), OLD + 1))
            .unwrap();
        let feed_offset = writer.verified_tail().unwrap().physical_tail;
        let feed = writer.append_synced(feed_input("0xfeed", OLD + 2)).unwrap();
        let feed_end = writer.verified_tail().unwrap().physical_tail;
        writer
            .append_synced(input("old-raw", noise(64_000, 3), OLD + 3))
            .unwrap();
        let evidence = (0..evidence_count)
            .map(|page| {
                writer
                    .append_synced(input(
                        "pin-evidence",
                        noise(16_000, u8::try_from(page).unwrap()),
                        OLD + 4,
                    ))
                    .unwrap()
            })
            .collect();
        let commitment = pe_service::bucket_commit::ActivityReadCommitment {
            version: 2,
            wallet: WalletAddress::from_hex(OTHER).unwrap(),
            fixed_end: NOW,
            digest: "empty".into(),
            bindings: Some(vec![]),
            read_proof: None,
        };
        let mut envelope = input(
            pe_service::bucket_commit::ACTIVITY_READ_COMMITMENT_SOURCE_ID,
            serde_json::to_vec(&commitment).unwrap(),
            NOW,
        );
        envelope.schema_version = 2;
        writer.append_synced(envelope).unwrap();
        writer
            .append_synced(input("recent-raw", b"recent".to_vec(), NOW))
            .unwrap();
        drop(writer);
        let bytes = fs::read(&path).unwrap();
        let feed_bytes = bytes
            [usize::try_from(feed_offset).unwrap()..usize::try_from(feed_end).unwrap()]
            .to_vec();
        let index = SourceReceiptIndex::replay(&path).unwrap();
        let state = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
        // Ordinary durable retirement lets the old feed be copied and physically erased.
        state.retire_activity_observation(feed, false).unwrap();
        let paper = Arc::new(PaperLog::open(dir.path().join("paper.log")).unwrap());
        if let Some(sealed) = sealed {
            let start_receipt = append_paper(&paper, &start());
            if sealed {
                let empty = TailBinding {
                    physical_tail: 5,
                    last_sequence: None,
                    last_hash: "00".repeat(32),
                };
                append_paper(
                    &paper,
                    &PaperLogRecord::QualificationSealed(Box::new(QualificationSealed {
                        start_receipt,
                        source_prefix: TailBinding::from(&activation),
                        financial_prefix: empty.clone(),
                        live_prefix: empty,
                        decision_evidence_digest: "digest".into(),
                        sealed_cutoff_unix: NOW,
                        reason: SealReason::InsufficientEvidence("fixture".into()),
                    })),
                );
            }
        }
        let seeds = Arc::new(AtomicBool::new(false));
        let (_, _, tx, control) =
            support::retention_controls(paper.as_ref().clone(), state.clone(), index.clone());
        let seed_flag = seeds.clone();
        let context = RetentionContext {
            paper_state: state.clone(),
            paper_log: paper.clone(),
            live_journal_path: None,
            control: tx,
            database_inputs: Arc::new(move || {
                Ok(RetentionDatabaseInputs {
                    identity_sequences: vec![],
                    dispatch_seeds_exist: seed_flag.load(Ordering::Acquire),
                })
            }),
            hooks: Arc::new(RetentionHooks::default()),
        };
        Self {
            _dir: dir,
            path,
            activation,
            index,
            state,
            seeds,
            context,
            control,
            feed,
            feed_bytes,
            feed_offset,
            feed_end,
            evidence,
        }
    }

    fn owner(&self, hooks: CheckpointOwnerHooks) -> SourceCheckpointOwner {
        let mut owner = SourceCheckpointOwner::for_retention_scenario(
            self.activation.clone(),
            self.index.clone(),
            CheckpointJobSlot::default(),
            false,
        )
        .unwrap()
        .with_retention(self.context.clone())
        .unwrap();
        let hooks = CheckpointOwnerHooks {
            clock: Some(Arc::new(|| Ok(u64::try_from(NOW).unwrap() * 1000))),
            ..hooks
        };
        owner.set_scenario_hooks(Arc::new(hooks));
        owner
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.control.abort();
    }
}

#[tokio::test]
async fn retention_crashes_finish_committed_epoch_before_any_new_advance() {
    for stage in ["feed", "commit", "publication", "punch"] {
        let mut fixture = Fixture::with_evidence(Some(true), if stage == "punch" { 2 } else { 0 });
        if stage == "punch" {
            let identities = vec![pe_core_types::EventSeq(3), fixture.evidence[1].sequence];
            fixture.context.database_inputs = Arc::new(move || {
                Ok(RetentionDatabaseInputs {
                    identity_sequences: identities.clone(),
                    dispatch_seeds_exist: false,
                })
            });
        }
        install_retention_fence(&fixture.path).unwrap();
        let fail = Arc::new(AtomicBool::new(true));
        let one_shot = fail.clone();
        let fault: Arc<dyn Fn() -> std::io::Result<()> + Send + Sync> = Arc::new(move || {
            if one_shot.swap(false, Ordering::AcqRel) {
                Err(std::io::Error::other("crash seam"))
            } else {
                Ok(())
            }
        });
        let mut hooks = RetentionHooks::default();
        match stage {
            "feed" => hooks.after_feed_copy = Some(fault),
            "commit" => hooks.after_commit = Some(fault),
            "publication" => hooks.after_publication = Some(fault),
            "punch" => hooks.mid_punch = Some(fault),
            _ => unreachable!(),
        }
        fixture.context.hooks = Arc::new(hooks);
        let mut owner = fixture.owner(CheckpointOwnerHooks::default());
        owner.initialize_for_scenario().await.unwrap();
        let header = fs::read(&fixture.path).unwrap()[..4096].to_vec();
        owner.retention_for_scenario().await.unwrap();
        assert_eq!(
            RetentionAuthority::load(&fixture.path).unwrap().is_some(),
            stage != "feed",
            "{stage}"
        );
        if stage == "commit" {
            assert_eq!(manifest(&fixture.path)["format_version"], 2);
        }
        if stage == "publication" || stage == "punch" {
            assert_eq!(manifest(&fixture.path)["format_version"], 3);
        }
        if stage == "punch" {
            let bytes = fs::read(&fixture.path).unwrap();
            assert!(bytes[32_768..36_864].iter().all(|byte| *byte == 0));
            let (_, end) = pe_event_log::Reader::read_at(
                &fixture.path,
                fixture.feed_end,
                pe_core_types::EventSeq(3),
                fixture.feed.this_hash,
            )
            .unwrap();
            let next_block = usize::try_from(end.div_ceil(4096) * 4096).unwrap();
            assert!(
                bytes[next_block..next_block + 4096]
                    .iter()
                    .any(|byte| *byte != 0)
            );
        }
        drop(owner);
        fixture.context.hooks = Arc::new(RetentionHooks::default());
        let mut restarted = fixture.owner(CheckpointOwnerHooks::default());
        restarted.initialize_for_scenario().await.unwrap();
        if stage == "feed" {
            restarted.retention_for_scenario().await.unwrap();
        }
        let authority = RetentionAuthority::load(&fixture.path).unwrap().unwrap();
        assert_eq!(authority.epoch, 1);
        assert_eq!(fixture.index.retention_epoch(), 1);
        assert_eq!(manifest(&fixture.path)["retention_epoch"], 1);
        assert_eq!(fs::read(&fixture.path).unwrap()[..4096], header);
        assert!(Scanner::verify(&fixture.path).is_ok());
        let entries =
            FeedArchiveIter::open(&fixture.path, &authority, &receipts_path(&fixture.path))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].seq, fixture.feed.sequence);
        assert_eq!(
            fs::read(pe_event_log::feed_path(&fixture.path, 1)).unwrap(),
            fixture.feed_bytes
        );
        let bytes = fs::read(&fixture.path).unwrap();
        if stage == "punch" {
            let (_, end) = pe_event_log::Reader::read_at(
                &fixture.path,
                fixture.feed_end,
                pe_core_types::EventSeq(3),
                fixture.feed.this_hash,
            )
            .unwrap();
            let next_block = usize::try_from(end.div_ceil(4096) * 4096).unwrap();
            assert!(
                bytes[next_block..next_block + 4096]
                    .iter()
                    .all(|byte| *byte == 0)
            );
        } else {
            assert!(
                bytes[usize::try_from(fixture.feed_offset).unwrap()
                    ..usize::try_from(fixture.feed_end).unwrap()]
                    .iter()
                    .all(|byte| *byte == 0)
            );
        }
    }
}

#[tokio::test]
async fn retention_publication_retry_keeps_archive_and_finishes_before_punch() {
    let fixture = Fixture::new(Some(true));
    install_retention_fence(&fixture.path).unwrap();
    let fail = Arc::new(AtomicBool::new(false));
    let flag = fail.clone();
    let mut owner = fixture.owner(CheckpointOwnerHooks {
        checkpoint_write: Some(Arc::new(move || {
            if flag.load(Ordering::Acquire) {
                Err(std::io::Error::other("publication unavailable"))
            } else {
                Ok(())
            }
        })),
        ..Default::default()
    });
    owner.initialize_for_scenario().await.unwrap();
    let original = fs::read(&fixture.path).unwrap();
    fail.store(true, Ordering::Release);
    owner.retention_for_scenario().await.unwrap();
    assert_eq!(
        RetentionAuthority::load(&fixture.path)
            .unwrap()
            .unwrap()
            .epoch,
        1
    );
    assert_eq!(manifest(&fixture.path)["format_version"], 2);
    let archive = fs::read(pe_event_log::feed_path(&fixture.path, 1)).unwrap();
    for _ in 0..2 {
        owner.retention_for_scenario().await.unwrap();
        assert_eq!(fs::read(&fixture.path).unwrap(), original);
    }
    fail.store(false, Ordering::Release);
    owner.retention_for_scenario().await.unwrap();
    assert_eq!(manifest(&fixture.path)["format_version"], 3);
    assert_ne!(fs::read(&fixture.path).unwrap(), original);
    assert_eq!(
        fs::read(pe_event_log::feed_path(&fixture.path, 1)).unwrap(),
        archive
    );
}

#[tokio::test]
async fn retention_publication_after_a_generation_change_ends_the_owner() {
    // Another process invalidated the checkpoint after the commit: the owner's candidate can never
    // publish, so the job fails the owner (restart) instead of retrying it forever.
    let fixture = Fixture::new(Some(true));
    install_retention_fence(&fixture.path).unwrap();
    let fail = Arc::new(AtomicBool::new(false));
    let flag = fail.clone();
    let mut owner = fixture.owner(CheckpointOwnerHooks {
        checkpoint_write: Some(Arc::new(move || {
            if flag.load(Ordering::Acquire) {
                Err(std::io::Error::other("publication unavailable"))
            } else {
                Ok(())
            }
        })),
        ..Default::default()
    });
    owner.initialize_for_scenario().await.unwrap();
    fail.store(true, Ordering::Release);
    owner.retention_for_scenario().await.unwrap();
    assert_eq!(
        RetentionAuthority::load(&fixture.path)
            .unwrap()
            .unwrap()
            .epoch,
        1
    );
    let original = fs::read(&fixture.path).unwrap();
    pe_service::source_checkpoint::invalidate(&fixture.path).unwrap();
    fail.store(false, Ordering::Release);
    let failure = owner.retention_for_scenario().await.unwrap_err();
    assert!(
        failure.message.contains("generation changed"),
        "{failure:?}"
    );
    assert_eq!(fs::read(&fixture.path).unwrap(), original);
}

#[tokio::test]
async fn retention_visible_authority_switches_index_while_sync_retries_fail() {
    let mut fixture = Fixture::new(Some(true));
    install_retention_fence(&fixture.path).unwrap();
    let fail_sync = Arc::new(AtomicBool::new(true));
    let sync_flag = fail_sync.clone();
    fixture.context.hooks = Arc::new(RetentionHooks {
        authority_write: Some(Arc::new(|authority, path| {
            authority.write(path)?;
            Err(pe_event_log::RetentionWriteError {
                visible: true,
                source: std::io::Error::other("after authority rename").into(),
            })
        })),
        directory_sync: Some(Arc::new(move |path| {
            if sync_flag.load(Ordering::Acquire) {
                Err(std::io::Error::other("directory unavailable").into())
            } else {
                RetentionAuthority::sync_directory(path)
            }
        })),
        ..Default::default()
    });
    let mut owner = fixture.owner(CheckpointOwnerHooks::default());
    owner.initialize_for_scenario().await.unwrap();
    let original = fs::read(&fixture.path).unwrap();
    let erased = fixture
        .index
        .receipt_at(pe_core_types::EventSeq(1))
        .unwrap()
        .unwrap()
        .0;
    let index = fixture.index.clone();
    let (entered, paused) = std::sync::mpsc::channel();
    let (release, resume) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        index.source_envelope_with_pause(erased, &mut || {
            entered.send(()).unwrap();
            resume.recv().unwrap();
        })
    });
    paused.recv().unwrap();
    owner.retention_for_scenario().await.unwrap();
    release.send(()).unwrap();
    assert_eq!(
        reader.join().unwrap().unwrap_err(),
        pe_service::risk_inputs::RiskInputsUnavailable::Erased
    );
    assert_eq!(fs::read(&fixture.path).unwrap(), original);
    assert_eq!(fixture.index.retention_epoch(), 1);
    let archive = fs::read(pe_event_log::feed_path(&fixture.path, 1)).unwrap();
    for _ in 0..2 {
        owner.retention_for_scenario().await.unwrap();
        assert_eq!(fs::read(&fixture.path).unwrap(), original);
    }
    assert_eq!(manifest(&fixture.path)["format_version"], 2);
    fail_sync.store(false, Ordering::Release);
    owner.retention_for_scenario().await.unwrap();
    assert_eq!(manifest(&fixture.path)["format_version"], 3);
    assert_eq!(
        fs::read(pe_event_log::feed_path(&fixture.path, 1)).unwrap(),
        archive
    );
}

#[test]
fn retention_database_inputs_include_all_identity_generations_and_finalized_seeds() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("paper.db");
    let state = PaperStateDb::open(&path).unwrap();
    let sql = rusqlite::Connection::open(&path).unwrap();
    for (generation, sequence) in [("previous", 1), ("current", 2)] {
        sql.execute(
            "INSERT INTO asset_identities VALUES (?1, '123', 'condition', 0, ?2, 'hash')",
            rusqlite::params![generation, sequence],
        )
        .unwrap();
    }
    let inputs = RetentionDatabaseInputs::read(&state).unwrap();
    assert_eq!(
        inputs.identity_sequences,
        vec![pe_core_types::EventSeq(1), pe_core_types::EventSeq(2)]
    );
    assert!(!inputs.dispatch_seeds_exist);
    sql.execute(
        "INSERT INTO dispatch_seeds VALUES ('dispatch', 'ready', '{}', NULL, 'trade', 1, 2)",
        [],
    )
    .unwrap();
    assert!(
        RetentionDatabaseInputs::read(&state)
            .unwrap()
            .dispatch_seeds_exist
    );
    drop(state);
}

#[tokio::test]
async fn retention_no_start_unsealed_and_dispatch_seed_pause_commit_and_finish() {
    for sealed in [None, Some(false), Some(true)] {
        let fixture = Fixture::new(sealed);
        install_retention_fence(&fixture.path).unwrap();
        if sealed == Some(true) {
            fixture.seeds.store(true, Ordering::Release);
        }
        let mut owner = fixture.owner(CheckpointOwnerHooks::default());
        owner.initialize_for_scenario().await.unwrap();
        owner.retention_for_scenario().await.unwrap();
        assert!(RetentionAuthority::load(&fixture.path).unwrap().is_none());
    }
    let mut fixture = Fixture::new(Some(true));
    install_retention_fence(&fixture.path).unwrap();
    let flag = fixture.seeds.clone();
    fixture.context.hooks = Arc::new(RetentionHooks {
        after_feed_copy: Some(Arc::new(move || {
            flag.store(true, Ordering::Release);
            Ok(())
        })),
        ..Default::default()
    });
    let mut owner = fixture.owner(CheckpointOwnerHooks::default());
    owner.initialize_for_scenario().await.unwrap();
    owner.retention_for_scenario().await.unwrap();
    assert!(RetentionAuthority::load(&fixture.path).unwrap().is_none());
    fixture.seeds.store(false, Ordering::Release);
    let flag = fixture.seeds.clone();
    fixture.context.hooks = Arc::new(RetentionHooks {
        after_commit: Some(Arc::new(move || {
            flag.store(true, Ordering::Release);
            Ok(())
        })),
        ..Default::default()
    });
    drop(owner);
    let mut owner = fixture.owner(CheckpointOwnerHooks::default());
    owner.initialize_for_scenario().await.unwrap();
    let original = fs::read(&fixture.path).unwrap();
    owner.retention_for_scenario().await.unwrap();
    assert_eq!(
        RetentionAuthority::load(&fixture.path)
            .unwrap()
            .unwrap()
            .epoch,
        1
    );
    assert_eq!(manifest(&fixture.path)["format_version"], 2);
    assert_eq!(fs::read(&fixture.path).unwrap(), original);
}

#[tokio::test]
async fn retention_incomplete_receipts_defer_without_authority_commit() {
    let fixture = Fixture::new(Some(true));
    install_retention_fence(&fixture.path).unwrap();
    let mut owner = fixture.owner(CheckpointOwnerHooks::default());
    owner.initialize_for_scenario().await.unwrap();
    fs::OpenOptions::new()
        .write(true)
        .open(receipts_path(&fixture.path))
        .unwrap()
        .set_len(80)
        .unwrap();
    owner.retention_for_scenario().await.unwrap();
    assert!(RetentionAuthority::load(&fixture.path).unwrap().is_none());
}

#[tokio::test]
async fn retention_authority_before_rename_leaves_epoch_and_index_unchanged() {
    let mut fixture = Fixture::new(Some(true));
    install_retention_fence(&fixture.path).unwrap();
    fixture.context.hooks = Arc::new(RetentionHooks {
        authority_write: Some(Arc::new(|_, _| {
            Err(pe_event_log::RetentionWriteError {
                visible: false,
                source: std::io::Error::other("before authority rename").into(),
            })
        })),
        ..Default::default()
    });
    let mut owner = fixture.owner(CheckpointOwnerHooks::default());
    owner.initialize_for_scenario().await.unwrap();
    let original = fs::read(&fixture.path).unwrap();
    owner.retention_for_scenario().await.unwrap();
    assert!(RetentionAuthority::load(&fixture.path).unwrap().is_none());
    assert_eq!(fixture.index.retention_epoch(), 0);
    assert_eq!(fs::read(&fixture.path).unwrap(), original);
    assert_eq!(manifest(&fixture.path)["format_version"], 2);
}

#[tokio::test]
async fn retention_digest_requires_the_complete_verified_captured_tail() {
    for truncate in [true, false] {
        let mut fixture = Fixture::new(Some(true));
        install_retention_fence(&fixture.path).unwrap();
        let original = fs::read(&fixture.path).unwrap();
        let path = fixture.path.clone();
        fixture.context.hooks = Arc::new(RetentionHooks {
            after_commit: Some(Arc::new(move || {
                if truncate {
                    let authority = RetentionAuthority::load(&path)
                        .map_err(std::io::Error::other)?
                        .unwrap();
                    fs::OpenOptions::new()
                        .write(true)
                        .open(&path)?
                        .set_len(authority.boundary.offset)?;
                } else {
                    let mut bytes = fs::read(&path)?;
                    *bytes.last_mut().unwrap() ^= 1;
                    fs::write(&path, bytes)?;
                }
                Ok(())
            })),
            ..Default::default()
        });
        let mut owner = fixture.owner(CheckpointOwnerHooks::default());
        owner.initialize_for_scenario().await.unwrap();
        owner.retention_for_scenario().await.unwrap();
        assert!(RetentionAuthority::load(&fixture.path).unwrap().is_some());
        assert_eq!(manifest(&fixture.path)["format_version"], 2);
        assert!(owner.retention_window_for_scenario().is_none());
        assert_eq!(
            &fs::read(&fixture.path).unwrap()[4096..8192],
            &original[4096..8192]
        );
        // Restore this fixture's coherent source capture; the committed epoch remains unfinished.
        fs::write(&fixture.path, &original).unwrap();
        owner.retention_for_scenario().await.unwrap();
        assert_eq!(manifest(&fixture.path)["retention_epoch"], 1);
        assert!(owner.retention_window_for_scenario().is_some());
    }
}

#[tokio::test]
async fn retention_fault_before_index_switch_still_installs_visible_boundary() {
    let mut fixture = Fixture::new(Some(true));
    install_retention_fence(&fixture.path).unwrap();
    fixture.context.hooks = Arc::new(RetentionHooks {
        before_index_switch: Some(Arc::new(|| Err(std::io::Error::other("switch seam")))),
        ..Default::default()
    });
    let mut owner = fixture.owner(CheckpointOwnerHooks::default());
    owner.initialize_for_scenario().await.unwrap();
    owner.retention_for_scenario().await.unwrap();
    assert_eq!(fixture.index.retention_epoch(), 1);
    assert_eq!(
        RetentionAuthority::load(&fixture.path)
            .unwrap()
            .unwrap()
            .epoch,
        1
    );
    assert_eq!(manifest(&fixture.path)["format_version"], 3);
}

#[tokio::test]
async fn retention_commit_defers_after_an_invalidation_since_the_capture() {
    // A concurrent preparation's invalidation can trim the receipts a prepared boundary needs.
    let mut fixture = Fixture::new(Some(true));
    install_retention_fence(&fixture.path).unwrap();
    let (proxy, mut requests) = tokio::sync::mpsc::channel(4);
    let orchestrator = fixture.context.control.clone();
    let path = fixture.path.clone();
    let forward = tokio::spawn(async move {
        while let Some(message) = requests.recv().await {
            let path = path.clone();
            tokio::task::spawn_blocking(move || pe_service::source_checkpoint::invalidate(&path))
                .await
                .unwrap()
                .unwrap();
            orchestrator.send(message).await.unwrap();
        }
    });
    fixture.context.control = proxy;
    let mut owner = fixture.owner(CheckpointOwnerHooks::default());
    owner.initialize_for_scenario().await.unwrap();
    owner.retention_for_scenario().await.unwrap();
    assert!(RetentionAuthority::load(&fixture.path).unwrap().is_none());
    assert_eq!(fixture.index.retention_epoch(), 0);
    forward.abort();
}

#[tokio::test]
async fn retention_deferred_check_rejects_a_corrupt_non_reducer_pin() {
    let fixture = Fixture::new(Some(true));
    install_retention_fence(&fixture.path).unwrap();
    let mut owner = fixture.owner(CheckpointOwnerHooks::default());
    owner.initialize_for_scenario().await.unwrap();
    owner.retention_for_scenario().await.unwrap();
    drop(owner);
    let authority = RetentionAuthority::load(&fixture.path).unwrap().unwrap();
    let pin = authority.pins.iter().find(|pin| !pin.reducer).unwrap();
    let mut owner = SourceCheckpointOwner::for_retention_scenario(
        fixture.activation.clone(),
        fixture.index.clone(),
        CheckpointJobSlot::default(),
        true,
    )
    .unwrap()
    .with_retention(fixture.context.clone())
    .unwrap();
    let prefix = fs::read(receipts_path(&fixture.path)).unwrap()
        [..usize::try_from(authority.boundary.sequence.0 * 80).unwrap()]
        .to_vec();
    let mut bytes = fs::read(&fixture.path).unwrap();
    bytes[usize::try_from(pin.offset + 20).unwrap()] ^= 1;
    fs::write(&fixture.path, bytes).unwrap();
    let error = owner.initialize_for_scenario().await.unwrap_err();
    assert!(error.message.contains("retained pin"), "{error:?}");
    assert!(!checkpoint_path(&fixture.path).exists());
    assert_eq!(fs::read(receipts_path(&fixture.path)).unwrap(), prefix);
    assert!(
        matches!(pe_service::source_checkpoint::read_authority(&fixture.path).unwrap(),
        pe_service::source_checkpoint::Authority::Readable(record) if record.active && record.retention_fence)
    );
}

#[tokio::test]
async fn retention_disposition_barrier_defers_until_wal_reader_releases() {
    let fixture = Fixture::new(Some(true));
    install_retention_fence(&fixture.path).unwrap();
    let mut owner = fixture.owner(CheckpointOwnerHooks::default());
    owner.initialize_for_scenario().await.unwrap();
    let reader = rusqlite::Connection::open(fixture._dir.path().join("paper.db")).unwrap();
    reader
        .execute_batch("BEGIN; SELECT count(*) FROM meta;")
        .unwrap();
    let receipt = fixture
        .index
        .receipt_at(pe_core_types::EventSeq(1))
        .unwrap()
        .unwrap()
        .0;
    fixture
        .state
        .retire_activity_observation(receipt, false)
        .unwrap();
    owner.retention_for_scenario().await.unwrap();
    assert!(RetentionAuthority::load(&fixture.path).unwrap().is_none());
    reader.execute_batch("ROLLBACK").unwrap();
    owner.retention_for_scenario().await.unwrap();
    assert_eq!(
        RetentionAuthority::load(&fixture.path)
            .unwrap()
            .unwrap()
            .epoch,
        1
    );
}

#[tokio::test]
async fn retention_source_referencing_live_record_pauses_advancing_and_finishing() {
    let mut fixture = Fixture::new(Some(true));
    install_retention_fence(&fixture.path).unwrap();
    let live_path = fixture._dir.path().join("live.log");
    let journal = pe_execution_core::LiveJournal::open(&live_path).unwrap();
    journal
        .append(
            pe_core_types::AccountId::new("retention-fixture").unwrap(),
            time::OffsetDateTime::from_unix_timestamp(NOW).unwrap(),
            pe_execution_core::live_journal::LiveJournalPayload::ResolutionFinalized(Box::new(
                pe_execution_core::live_journal::ResolutionFinalizedAudit {
                    condition_id: pe_core_types::PolymarketConditionId("condition".into()),
                    payout_by_outcome_index_json: "[1,0]".into(),
                    source_append_receipt: fixture.feed,
                },
            )),
        )
        .unwrap();
    fixture.context.live_journal_path = Some(live_path.clone());
    let mut owner = fixture.owner(CheckpointOwnerHooks::default());
    owner.initialize_for_scenario().await.unwrap();
    owner.retention_for_scenario().await.unwrap();
    assert!(RetentionAuthority::load(&fixture.path).unwrap().is_none());
    drop(owner);
    fixture.context.live_journal_path = None;
    fixture.context.hooks = Arc::new(RetentionHooks {
        after_commit: Some(Arc::new(|| Err(std::io::Error::other("stop after commit")))),
        ..Default::default()
    });
    let mut owner = fixture.owner(CheckpointOwnerHooks::default());
    owner.initialize_for_scenario().await.unwrap();
    owner.retention_for_scenario().await.unwrap();
    assert_eq!(
        RetentionAuthority::load(&fixture.path)
            .unwrap()
            .unwrap()
            .epoch,
        1
    );
    drop(owner);
    fixture.context.live_journal_path = Some(live_path);
    fixture.context.hooks = Arc::new(RetentionHooks::default());
    let original = fs::read(&fixture.path).unwrap();
    let mut owner = fixture.owner(CheckpointOwnerHooks::default());
    owner.initialize_for_scenario().await.unwrap();
    assert_eq!(manifest(&fixture.path)["format_version"], 2);
    assert_eq!(fs::read(&fixture.path).unwrap(), original);
}

#[tokio::test]
async fn retention_verified_wallet_witnesses_are_ephemeral_and_rewalked_without_advance() {
    let fixture = Fixture::new(Some(true));
    install_retention_fence(&fixture.path).unwrap();
    let mut owner = fixture.owner(CheckpointOwnerHooks::default());
    owner.initialize_for_scenario().await.unwrap();
    owner.retention_for_scenario().await.unwrap();
    let other = WalletAddress::from_hex(OTHER).unwrap();
    let departed = WalletAddress::from_hex(WALLET).unwrap();
    assert!(
        owner
            .retention_window_for_scenario()
            .unwrap()
            .contains(&other)
    );
    assert!(
        !owner
            .retention_window_for_scenario()
            .unwrap()
            .contains(&departed)
    );
    assert!(manifest(&fixture.path).get("wallet_positions").is_none());
    let future = NOW + 9 * 24 * 3600;
    owner.set_scenario_hooks(Arc::new(CheckpointOwnerHooks {
        clock: Some(Arc::new(move || Ok(u64::try_from(future).unwrap() * 1000))),
        ..Default::default()
    }));
    owner.retention_for_scenario().await.unwrap();
    let authority = RetentionAuthority::load(&fixture.path).unwrap().unwrap();
    assert_eq!(authority.boundary.sequence.0, 5);
    assert!(owner.retention_window_for_scenario().unwrap().is_empty());
    // B is already the last frame: the next daily job must still walk the retained window.
    owner.set_scenario_hooks(Arc::new(CheckpointOwnerHooks {
        clock: Some(Arc::new(move || {
            Ok(u64::try_from(future + 24 * 3600).unwrap() * 1000)
        })),
        ..Default::default()
    }));
    owner.retention_for_scenario().await.unwrap();
    assert_eq!(
        RetentionAuthority::load(&fixture.path)
            .unwrap()
            .unwrap()
            .epoch,
        authority.epoch
    );
    assert!(owner.retention_window_for_scenario().unwrap().is_empty());
}

#[tokio::test]
async fn retention_checkpoint_prune_drops_routed_marker_after_its_obligation_retires() {
    let mut fixture = Fixture::new(Some(true));
    let artifact = pe_service::frame_admission::FrameFallbackArtifact {
        version: 1,
        frame_receipt: fixture.feed,
        routing_clock: time::OffsetDateTime::from_unix_timestamp(NOW).unwrap(),
        reason: pe_service::frame_admission::FrameFallbackReason::HistoryBehind,
        frontier: None,
        latest_incident_basis: pe_service::frame_admission::FeedLatchBasis::default(),
    };
    let mut writer = Writer::open(&fixture.path).unwrap();
    writer
        .append_synced(input(
            pe_service::frame_admission::FRAME_FALLBACK_SOURCE_ID,
            serde_json::to_vec(&artifact).unwrap(),
            NOW,
        ))
        .unwrap();
    drop(writer);
    fixture.index = SourceReceiptIndex::replay(&fixture.path).unwrap();
    let mut owner = fixture.owner(CheckpointOwnerHooks::default());
    owner.initialize_for_scenario().await.unwrap();
    assert_eq!(
        manifest(&fixture.path)["activity"]["routed_frames"],
        serde_json::json!([])
    );
}

#[tokio::test]
async fn retention_pins_financial_inputs_recent_decisions_and_commit_time_updates() {
    use pe_core_types::SourceTradeId;
    use pe_service::paper_recovery::{
        CanonicalFillResult, ExpectedAuthority, FinancialPayload, FinancialResult,
        PaperFillOperationIdentity,
    };
    for case in [
        "pending",
        "old_completed",
        "recent_completed",
        "updated_during_copy",
    ] {
        let mut fixture = Fixture::with_evidence(Some(true), 8);
        install_retention_fence(&fixture.path).unwrap();
        let start_receipt = fixture.context.paper_log.snapshot().unwrap()[0].receipt;
        let mut economic = support::economic_prepared(fixture.evidence[6], start_receipt);
        economic.admission.receipts.gamma = fixture.evidence[0];
        economic.admission.receipts.clob_long = fixture.evidence[1];
        economic.admission.receipts.clob_compact = fixture.evidence[2];
        economic.book_receipt = fixture.evidence[3];
        economic.risk.price_receipts = vec![fixture.evidence[4], fixture.evidence[5]];
        economic
            .observation
            .as_mut()
            .unwrap()
            .complete_bound_receipt = fixture.evidence[7];
        let prepared = append_paper(
            &fixture.context.paper_log,
            &PaperLogRecord::FinancialPrepared {
                expected_authority: ExpectedAuthority {
                    qualification_start_receipt: start_receipt,
                    prior_completed_prepared_sequence: None,
                },
                payload: FinancialPayload::Fill {
                    operation: PaperFillOperationIdentity {
                        leader_wallet: WalletAddress::from_hex(WALLET).unwrap(),
                        source_trade_id: SourceTradeId("g2:fill".into()),
                        observed_at_bucket: OLD,
                    },
                    economic: economic.clone(),
                },
            },
        );
        if case != "pending" {
            append_paper(
                &fixture.context.paper_log,
                &PaperLogRecord::FinancialFinal {
                    prepared_receipt: prepared,
                    result: FinancialResult::Fill {
                        canonical: CanonicalFillResult {
                            outcome: "applied".into(),
                            bankroll: rust_decimal::Decimal::from(9),
                            applied_prepared_seq: prepared.sequence,
                            quantity: economic.sizing.expected_shares,
                            principal: economic.sizing.principal,
                            fee: economic.fee.expected_fee,
                            fill_price: economic.ladder.limit_price,
                        },
                    },
                },
            );
            let connection =
                rusqlite::Connection::open(fixture._dir.path().join("paper.db")).unwrap();
            connection
                .execute(
                    "INSERT INTO meta (key, value) VALUES ('financial_last_prepared_seq', ?1)",
                    [i64::try_from(prepared.sequence.0).unwrap()],
                )
                .unwrap();
            let facts = support::legacy_continuation_wire(2);
            connection
                .execute(
                    "INSERT INTO decision_pending (source_trade_id, semantic_revision, wallet_hex, source_epoch, frozen_inputs_json, post_commit_inputs_json, state, terminal_disposition, updated_at_unix) VALUES ('g2:fill', 'semantic-v2', ?1, 1700000000, ?2, ?3, 'terminal', 'fill', ?4)",
                    rusqlite::params![
                        WALLET,
                        facts.to_string(),
                        include_str!("fixtures/decision_replay_origin_main_v2_terminal.json"),
                        if case == "recent_completed" { NOW } else { OLD },
                    ],
                )
                .unwrap();
            assert_eq!(
                fixture.state.financial_last_prepared_seq().unwrap(),
                Some(prepared.sequence)
            );
            if case == "updated_during_copy" {
                let path = fixture._dir.path().join("paper.db");
                fixture.context.hooks = Arc::new(RetentionHooks {
                    after_feed_copy: Some(Arc::new(move || {
                        rusqlite::Connection::open(&path)
                            .and_then(|connection| {
                                connection.execute(
                                    "UPDATE decision_pending SET updated_at_unix = ?1",
                                    [NOW],
                                )
                            })
                            .map_err(std::io::Error::other)?;
                        Ok(())
                    })),
                    ..Default::default()
                });
            }
        }
        let mut owner = fixture.owner(CheckpointOwnerHooks::default());
        owner.initialize_for_scenario().await.unwrap();
        owner.retention_for_scenario().await.unwrap();
        let authority = RetentionAuthority::load(&fixture.path).unwrap().unwrap();
        for receipt in &fixture.evidence {
            if case == "old_completed" {
                assert!(authority.pin(receipt.sequence).is_none(), "{case}");
            } else {
                let pin = authority.pin(receipt.sequence).unwrap();
                assert!(!pin.reducer);
                assert_eq!(
                    authority
                        .verify_pin(&fixture.path, pin)
                        .unwrap()
                        .0
                        .this_hash,
                    receipt.this_hash
                );
            }
        }
    }
}

#[tokio::test]
async fn retention_pins_membership_and_identity_artifacts_since_start() {
    use pe_service::paper_recovery::MembershipReason;
    let mut fixture = Fixture::with_evidence(Some(true), 7);
    install_retention_fence(&fixture.path).unwrap();
    let wallet = WalletAddress::from_hex(WALLET).unwrap();
    for (reason, evidence) in [
        (
            MembershipReason::FullRerank,
            serde_json::json!({
                "kind": "full_rerank", "ranking_receipt": fixture.evidence[0],
                "admission_receipts": [{"wallet": wallet, "receipt": fixture.evidence[1]}],
            }),
        ),
        (
            MembershipReason::KnockoutInactivity,
            serde_json::json!({
                "kind": "knockout_backfill", "ranking_receipt": fixture.evidence[2],
                "evictions": [{"wallet": wallet, "reason": "knockout_inactivity", "causal_receipt": fixture.evidence[3]}],
                "admission_receipts": [],
            }),
        ),
        (
            MembershipReason::CapacityChange,
            serde_json::json!({
                "kind": "capacity_change", "generation": 1, "config_receipt": fixture.evidence[4],
                "admission_receipts": [{"wallet": wallet, "receipt": fixture.evidence[5]}],
            }),
        ),
    ] {
        append_paper(
            &fixture.context.paper_log,
            &PaperLogRecord::MembershipChanged {
                reason,
                removed: vec![],
                added: vec![wallet],
                capacity: 1,
                ranking_batch_id: None,
                evidence,
            },
        );
    }
    let identity = fixture.evidence[6].sequence;
    fixture.context.database_inputs = Arc::new(move || {
        Ok(RetentionDatabaseInputs {
            identity_sequences: vec![identity],
            dispatch_seeds_exist: false,
        })
    });
    let mut owner = fixture.owner(CheckpointOwnerHooks::default());
    owner.initialize_for_scenario().await.unwrap();
    owner.retention_for_scenario().await.unwrap();
    let authority = RetentionAuthority::load(&fixture.path).unwrap().unwrap();
    for receipt in &fixture.evidence {
        let pin = authority.pin(receipt.sequence).unwrap();
        assert!(!pin.reducer);
        assert_eq!(
            authority
                .verify_pin(&fixture.path, pin)
                .unwrap()
                .0
                .this_hash,
            receipt.this_hash
        );
    }
}

#[tokio::test]
async fn retention_obligation_pins_authenticated_closure_without_a_decision_owner() {
    let mut fixture = Fixture::new(Some(true));
    let connection = rusqlite::Connection::open(fixture._dir.path().join("paper.db")).unwrap();
    connection
        .execute(
            "DELETE FROM meta WHERE key = 'retired_activity_observation:2'",
            [],
        )
        .unwrap();
    assert!(
        !fixture
            .state
            .activity_observation_retired(fixture.feed)
            .unwrap()
    );
    let boundary_offset = pe_event_log::Reader::read_at(
        &fixture.path,
        fixture.feed_end,
        pe_core_types::EventSeq(3),
        fixture.feed.this_hash,
    )
    .unwrap()
    .1;
    fs::OpenOptions::new()
        .write(true)
        .open(&fixture.path)
        .unwrap()
        .set_len(boundary_offset)
        .unwrap();
    let wallet = WalletAddress::from_hex(WALLET).unwrap();
    let mut writer = Writer::open(&fixture.path).unwrap();
    let payload = serde_json::to_vec(&serde_json::json!([{
        "proxyWallet": WALLET, "conditionId": format!("0x{}", "11".repeat(32)),
        "asset": "123", "side": "BUY", "size": "100", "price": "0.50", "usdcSize": "50",
        "timestamp": OLD + 1, "transactionHash": "0xfeed", "outcomeIndex": 0, "type": "TRADE",
    }]))
    .unwrap();
    let (read, _) =
        support::append_committed_read_v2(&mut writer, wallet, &payload, OLD + 10, OLD + 10);
    let proof: serde_json::Value = serde_json::from_str(&read.decision_inputs_json).unwrap();
    let pages: Vec<pe_source_polymarket_public::ReconciliationPageEvidence> =
        serde_json::from_value(proof["pages"].clone()).unwrap();
    let target = &read.aggregates[0];
    let binding = pe_service::bucket_commit::ObservationBinding {
        stream_group_id: target.group_id.key().clone(),
        stream_receipt: fixture.feed,
        history_group_id: target.group_id.key().clone(),
        semantic_revision: target.semantic_revision.as_str().to_owned(),
        page_raw_hash: read.page.raw_hash.clone(),
        page_occurrence_index: 0,
        identity_provenance: None,
        identity_receipt: None,
        counterpart_basis_receipt: None,
        frame_admission_receipt: None,
    };
    let payload = pe_service::bucket_commit::activity_read_commitment_payload_v2(
        wallet,
        OLD + 10,
        std::slice::from_ref(&read.page),
        &pages,
        &[binding],
    )
    .unwrap();
    let mut envelope = input(
        pe_service::bucket_commit::ACTIVITY_READ_COMMITMENT_SOURCE_ID,
        payload,
        OLD + 10,
    );
    envelope.schema_version = 2;
    let commitment = writer.append_synced(envelope).unwrap();
    let artifact = pe_service::frame_admission::FrameFallbackArtifact {
        version: 1,
        frame_receipt: fixture.feed,
        routing_clock: time::OffsetDateTime::from_unix_timestamp(OLD + 11).unwrap(),
        reason: pe_service::frame_admission::FrameFallbackReason::HistoryBehind,
        frontier: None,
        latest_incident_basis: pe_service::frame_admission::FeedLatchBasis::default(),
    };
    let routed = writer
        .append_synced(input(
            pe_service::frame_admission::FRAME_FALLBACK_SOURCE_ID,
            serde_json::to_vec(&artifact).unwrap(),
            OLD + 11,
        ))
        .unwrap();
    writer
        .append_synced(input("recent", b"recent".to_vec(), NOW))
        .unwrap();
    drop(writer);
    fixture.index = SourceReceiptIndex::replay(&fixture.path).unwrap();
    install_retention_fence(&fixture.path).unwrap();
    assert!(fixture.state.decision_pending_history().unwrap().is_empty());
    let mut owner = fixture.owner(CheckpointOwnerHooks::default());
    owner.initialize_for_scenario().await.unwrap();
    owner.retention_for_scenario().await.unwrap();
    let authority = RetentionAuthority::load(&fixture.path).unwrap().unwrap();
    for (receipt, reducer) in [
        (fixture.feed, true),
        (commitment, true),
        (routed, true),
        (read.page.receipt, false),
    ] {
        let pin = authority.pin(receipt.sequence).unwrap();
        assert_eq!(pin.reducer, reducer);
        assert_eq!(
            pin.wallet,
            if receipt == fixture.feed {
                Some(wallet)
            } else {
                None
            }
        );
        assert_eq!(
            authority
                .verify_pin(&fixture.path, pin)
                .unwrap()
                .0
                .this_hash,
            receipt.this_hash
        );
    }
    assert_eq!(
        manifest(&fixture.path)["activity"]["routed_frames"],
        serde_json::json!([fixture.feed.sequence.0])
    );
}

#[tokio::test]
async fn retention_waits_until_the_owner_snapshot_reaches_past_the_boundary() {
    // Frames appended after the owner's capture can become the boundary: an old-dated feed
    // observation its reducers never saw (with receipts published by a concurrent preparation),
    // or a lone current frame starting exactly at the frozen tail.
    for late_observation in [true, false] {
        let mut fixture = Fixture::new(Some(true));
        let old_end = pe_event_log::Reader::read_at(
            &fixture.path,
            fixture.feed_end,
            pe_core_types::EventSeq(3),
            fixture.feed.this_hash,
        )
        .unwrap()
        .1;
        fs::OpenOptions::new()
            .write(true)
            .open(&fixture.path)
            .unwrap()
            .set_len(old_end)
            .unwrap();
        fixture.index = SourceReceiptIndex::replay(&fixture.path).unwrap();
        install_retention_fence(&fixture.path).unwrap();
        let mut owner = fixture.owner(CheckpointOwnerHooks::default());
        owner.initialize_for_scenario().await.unwrap();
        let mut writer = Writer::open(&fixture.path).unwrap();
        let mut appended = Vec::new();
        if late_observation {
            let receipt = writer.append_synced(feed_input("0xlate", OLD + 4)).unwrap();
            owner
                .record_synced_append_for_scenario(receipt, &feed_input("0xlate", OLD + 4))
                .unwrap();
            appended.push(receipt);
        }
        let receipt = writer
            .append_synced(input("recent", b"recent".to_vec(), NOW))
            .unwrap();
        owner
            .record_synced_append_for_scenario(receipt, &input("recent", b"recent".to_vec(), NOW))
            .unwrap();
        appended.push(receipt);
        drop(writer);
        if late_observation {
            let mut preparation = fixture.owner(CheckpointOwnerHooks::default());
            preparation.initialize_for_scenario().await.unwrap();
        }
        owner.retention_for_scenario().await.unwrap();
        assert!(
            RetentionAuthority::load(&fixture.path).unwrap().is_none(),
            "{late_observation}"
        );
        owner.publish_hourly_for_scenario().await.unwrap();
        owner.retention_for_scenario().await.unwrap();
        let authority = RetentionAuthority::load(&fixture.path).unwrap().unwrap();
        assert_eq!(authority.epoch, 1);
        assert_eq!(authority.boundary.sequence, receipt.sequence);
        assert_eq!(manifest(&fixture.path)["retention_epoch"], 1);
        assert!(Scanner::verify(&fixture.path).is_ok());
        if late_observation {
            let pin = authority.pin(appended[0].sequence).unwrap();
            assert!(pin.reducer);
            assert_eq!(pin.wallet, Some(WalletAddress::from_hex(WALLET).unwrap()));
        }
    }
}
