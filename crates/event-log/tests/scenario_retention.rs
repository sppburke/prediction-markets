#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use pe_core_types::{EventSeq, ReceivedAt, SourceId, SourceTimestamp, WalletAddress};
use pe_event_log::{
    AppendReceipt, CheckpointPrefix, ContentType, EnvelopeIn, EventEnvelope, FeedArchiveIter,
    FeedArchiveWriter, LogError, LogTailBinding, Reader, ReceiptRecord, RetentionAuthority,
    RetentionBoundary, RetentionPin, Scanner, TailBinding, Writer, feed_path, retention_path,
};
use tempfile::TempDir;
use time::OffsetDateTime;

struct Fixture {
    _directory: TempDir,
    path: PathBuf,
    receipts: PathBuf,
    frames: Vec<(u64, EventSeq, EventEnvelope)>,
    bytes: Vec<u8>,
    authority: RetentionAuthority,
}

impl Fixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("source.log");
        let receipts = directory.path().join("receipts");
        let mut writer = Writer::open(&path).unwrap();
        for number in 0..8 {
            writer.append_synced(input(number)).unwrap();
        }
        drop(writer);
        let frames: Vec<_> = Reader::replay_with_offsets(&path)
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        let mut records = File::create(&receipts).unwrap();
        for (offset, sequence, envelope) in &frames {
            records
                .write_all(
                    &ReceiptRecord {
                        receipt: AppendReceipt {
                            sequence: *sequence,
                            this_hash: envelope.this_hash,
                        },
                        received_millis: 0,
                        byte_offset: Some(*offset),
                    }
                    .encode(),
                )
                .unwrap();
        }
        records.sync_all().unwrap();
        let bytes = std::fs::read(&path).unwrap();
        let authority = RetentionAuthority {
            format_version: 1,
            epoch: 1,
            advanced_at: 1_700_000_000,
            boundary: RetentionBoundary {
                sequence: EventSeq(3),
                offset: frames[3].0,
            },
            chain_head: frames[2].2.this_hash,
            pins: vec![pin(&frames[1]), pin(&frames[2])],
            retained_tail: TailBinding::from(&Scanner::verify(&path).unwrap()),
            feed: vec![],
        };
        Self {
            _directory: directory,
            path,
            receipts,
            frames,
            bytes,
            authority,
        }
    }

    fn commit(&self) {
        self.authority.write(&self.path).unwrap();
    }

    fn erase(&self) {
        let mut bytes = self.bytes.clone();
        for (offset, sequence, _) in &self.frames[..3] {
            if self.authority.pin(*sequence).is_none() {
                let end = self.binding(sequence.0).physical_tail;
                bytes[usize::try_from(*offset).unwrap()..usize::try_from(end).unwrap()].fill(0);
            }
        }
        std::fs::write(&self.path, bytes).unwrap();
    }

    fn binding(&self, sequence: u64) -> LogTailBinding {
        let index = usize::try_from(sequence).unwrap();
        LogTailBinding {
            path: std::fs::canonicalize(&self.path).unwrap(),
            physical_tail: self
                .frames
                .get(index + 1)
                .map_or(u64::try_from(self.bytes.len()).unwrap(), |frame| frame.0),
            last_sequence: Some(EventSeq(sequence)),
            last_hash: self.frames[index].2.this_hash,
        }
    }

    fn archive(&mut self) {
        let mut writer = FeedArchiveWriter::new(&self.path, 1).unwrap();
        for index in [0, 2] {
            let frame = &self.frames[index];
            writer.append(frame.0, frame.1, frame.2.prev_hash).unwrap();
        }
        self.authority.feed.push(writer.finish().unwrap().unwrap());
    }

    fn feeds(&self) -> Result<Vec<EventEnvelope>, LogError> {
        FeedArchiveIter::open(&self.path, &self.authority, &self.receipts)?.collect()
    }
}

fn pin(frame: &(u64, EventSeq, EventEnvelope)) -> RetentionPin {
    RetentionPin {
        sequence: frame.1,
        offset: frame.0,
        hash: frame.2.this_hash,
        predecessor_hash: frame.2.prev_hash,
        reducer: false,
        wallet: None,
    }
}

fn input(number: u8) -> EnvelopeIn {
    EnvelopeIn {
        source_id: SourceId("retention-fixture".into()),
        schema_version: 1,
        parser_version: 1,
        observed_at: SourceTimestamp(OffsetDateTime::UNIX_EPOCH),
        received_at: ReceivedAt(OffsetDateTime::UNIX_EPOCH),
        content_type: ContentType::Raw,
        payload: vec![number; 200],
    }
}

#[test]
fn retention_authority_round_trip_and_corruption() {
    let mut fixture = Fixture::new();
    assert!(RetentionAuthority::load(&fixture.path).unwrap().is_none());
    fixture.authority.pins[0].reducer = true;
    fixture.authority.pins[0].wallet = Some(WalletAddress([7; 20]));
    fixture.commit();
    assert_eq!(
        RetentionAuthority::load(&fixture.path).unwrap(),
        Some(fixture.authority.clone())
    );
    let path = retention_path(&fixture.path);
    let original = std::fs::read(&path).unwrap();
    let mut json: serde_json::Value = serde_json::from_slice(&original).unwrap();
    json["authority"]["epoch"] = 2.into();
    std::fs::write(&path, serde_json::to_vec(&json).unwrap()).unwrap();
    assert!(matches!(
        RetentionAuthority::load(&fixture.path),
        Err(pe_event_log::RetentionError::Invalid(_))
    ));
    std::fs::write(&path, b"garbage").unwrap();
    assert!(RetentionAuthority::load(&fixture.path).is_err());
    assert!(Scanner::verify(&fixture.path).is_err());
    let before = std::fs::read(&fixture.path).unwrap();
    assert!(Writer::open(&fixture.path).is_err());
    assert_eq!(std::fs::read(&fixture.path).unwrap(), before);
}

#[test]
fn retention_authority_rejects_invalid_structure_before_publication() {
    let fixture = Fixture::new();
    let mut invalid = fixture.authority.clone();
    for change in 0..6 {
        invalid.clone_from(&fixture.authority);
        match change {
            0 => invalid.format_version = 99,
            1 => invalid.epoch = 0,
            2 => invalid.pins.reverse(),
            3 => invalid.pins.push(invalid.pins[1].clone()),
            4 => invalid.retained_tail.physical_tail = invalid.boundary.offset,
            _ => invalid.pins[0].wallet = Some(WalletAddress([7; 20])),
        }
        let error = invalid.write(&fixture.path).unwrap_err();
        assert!(!error.visible);
        assert!(RetentionAuthority::load(&fixture.path).unwrap().is_none());
    }
}

#[test]
fn retention_seeded_inspect_verify_walk_hash_and_replay() {
    let mut fixture = Fixture::new();
    fixture.authority.pins.clear();
    fixture.commit();
    fixture.erase(); // Every frame below the boundary is zero-filled.
    let tail = fixture.binding(7);
    assert_eq!(Scanner::inspect(&fixture.path).unwrap().verified_tail, tail);
    assert_eq!(Scanner::verify(&fixture.path).unwrap(), tail);
    assert_eq!(Scanner::verify_prefix(&tail).unwrap(), tail);
    let mut seen = vec![];
    assert_eq!(
        Scanner::walk_prefix(&tail, &mut |_, envelope| seen.push(envelope.seq)).unwrap(),
        Some(tail.clone())
    );
    assert_eq!(seen, (3..8).map(EventSeq).collect::<Vec<_>>());
    let mut digest = blake3::Hasher::new();
    let mut seen = vec![];
    let cancel = AtomicBool::new(false);
    assert_eq!(
        Scanner::walk_bounded_cancellable(
            &fixture.path,
            tail.physical_tail,
            &tail,
            None,
            &mut digest,
            &mut |offset, envelope| seen.push((offset, envelope.seq)),
            Some(&cancel)
        )
        .unwrap(),
        tail
    );
    assert_eq!(seen[0], (fixture.authority.boundary.offset, EventSeq(3)));
    let expected =
        blake3::hash(&fixture.bytes[usize::try_from(fixture.authority.boundary.offset).unwrap()..]);
    assert_eq!(digest.finalize(), expected);
    assert_eq!(
        Scanner::hash_prefix(&fixture.path, tail.physical_tail)
            .unwrap()
            .finalize(),
        expected
    );
    assert_eq!(
        Scanner::hash_prefix_cancellable(&fixture.path, tail.physical_tail, Some(&cancel))
            .unwrap()
            .finalize(),
        expected
    );
    assert_eq!(
        Reader::replay(&fixture.path)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .len(),
        5
    );
    assert_eq!(
        Reader::replay_with_offsets(&fixture.path)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .0,
        fixture.authority.boundary.offset
    );
    assert_eq!(Reader::verified_tail(&fixture.path).unwrap(), tail);
    assert_eq!(
        Reader::tail(&fixture.path, Duration::from_millis(1))
            .unwrap()
            .take(5)
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .len(),
        5
    );
    let mut writer = Writer::open(&fixture.path).unwrap();
    assert_eq!(
        writer.append_synced(input(9)).unwrap().sequence,
        EventSeq(8)
    );
}

#[test]
fn retention_pinned_and_unpinned_bindings_on_fresh_and_resumed_walks() {
    let fixture = Fixture::new();
    fixture.commit();
    fixture.erase();
    let activation = fixture.binding(1);
    let checkpoint = fixture.binding(5);
    let tail = fixture.binding(7);
    assert_eq!(Scanner::verify_prefix(&activation).unwrap(), tail);
    fixture.authority.verify_pins(&fixture.path).unwrap();
    for resume in [None, Some(&checkpoint)] {
        let mut digest = Scanner::hash_prefix(&fixture.path, checkpoint.physical_tail).unwrap();
        assert_eq!(
            Scanner::walk_bounded(
                &fixture.path,
                tail.physical_tail,
                &activation,
                resume,
                &mut digest,
                &mut |_, _| {}
            )
            .unwrap(),
            tail
        );
        assert!(
            Scanner::walk_bounded(
                &fixture.path,
                tail.physical_tail,
                &fixture.binding(0),
                resume,
                &mut digest,
                &mut |_, _| {}
            )
            .is_err()
        );
        for mismatch in 0..3 {
            let mut bad = activation.clone();
            match mismatch {
                0 => bad.physical_tail -= 1,
                1 => bad.last_hash = blake3::hash(b"wrong"),
                _ => bad.path = PathBuf::from("/wrong/source.log"),
            }
            assert!(
                Scanner::walk_bounded(
                    &fixture.path,
                    tail.physical_tail,
                    &bad,
                    resume,
                    &mut digest,
                    &mut |_, _| {}
                )
                .is_err()
            );
        }
    }
    assert!(Scanner::verify_prefix(&fixture.binding(0)).is_err());
    // Even valid metadata cannot authenticate a pin whose bytes were punched.
    zero_range(&fixture.path, fixture.frames[1].0, fixture.frames[2].0);
    assert!(fixture.authority.verify_pins(&fixture.path).is_err());
    assert!(Scanner::verify_prefix(&activation).is_err());
    let mut digest = blake3::Hasher::new();
    assert!(
        Scanner::walk_bounded(
            &fixture.path,
            tail.physical_tail,
            &activation,
            Some(&checkpoint),
            &mut digest,
            &mut |_, _| {}
        )
        .is_err()
    );
}

#[test]
fn retention_walk_prefix_returns_retired_at_or_below_boundary() {
    let fixture = Fixture::new();
    fixture.commit();
    for seq in 0..3 {
        let mut observed = 0;
        assert!(matches!(
            Scanner::walk_prefix(&fixture.binding(seq), &mut |_, _| observed += 1),
            Err(LogError::Retired { .. })
        ));
        assert_eq!(observed, 0);
    }
}

#[test]
fn retention_writer_opens_and_incremental_digest_use_the_window() {
    let fixture = Fixture::new();
    fixture.commit();
    fixture.erase();
    let activation = fixture.binding(1);
    let checkpoint = fixture.binding(5);
    let tail = fixture.binding(7);
    drop(Writer::open_with_expected_tail(&fixture.path, &tail).unwrap());
    let mut observed = vec![];
    let (writer, verified) =
        Writer::open_verified(&fixture.path, Some(&activation), &mut |_, frame| {
            observed.push(frame.seq)
        })
        .unwrap();
    assert_eq!(verified, tail);
    assert_eq!(observed, (3..8).map(EventSeq).collect::<Vec<_>>());
    drop(writer);
    let prefix = Scanner::hash_prefix(&fixture.path, checkpoint.physical_tail)
        .unwrap()
        .finalize()
        .to_hex()
        .to_string();
    for mode in [CheckpointPrefix::Verify, CheckpointPrefix::Defer] {
        let mut digest = blake3::Hasher::new();
        let mut observed = vec![];
        let (writer, binding, verification) = Writer::open_verified_checkpoint(
            &fixture.path,
            &activation,
            Some((&checkpoint, &prefix)),
            mode,
            &mut digest,
            &mut |used| assert!(used),
            &mut |_, frame| observed.push(frame.seq),
        )
        .unwrap();
        assert!(verification.used);
        assert_eq!(observed, vec![EventSeq(6), EventSeq(7)]);
        assert_eq!(binding, tail);
        let start = if mode == CheckpointPrefix::Verify {
            fixture.authority.boundary.offset
        } else {
            checkpoint.physical_tail
        };
        assert_eq!(
            digest.finalize(),
            blake3::hash(&fixture.bytes[usize::try_from(start).unwrap()..])
        );
        drop(writer);
    }
    let mut digest = Scanner::hash_prefix(&fixture.path, checkpoint.physical_tail).unwrap();
    assert_eq!(
        Scanner::walk_bounded(
            &fixture.path,
            tail.physical_tail,
            &activation,
            Some(&checkpoint),
            &mut digest,
            &mut |_, _| {}
        )
        .unwrap(),
        tail
    );
    assert_eq!(
        digest.finalize(),
        Scanner::hash_prefix(&fixture.path, tail.physical_tail)
            .unwrap()
            .finalize()
    );
}

fn zero_range(path: &Path, start: u64, end: u64) {
    let mut file = OpenOptions::new().write(true).open(path).unwrap();
    file.seek(SeekFrom::Start(start)).unwrap();
    file.write_all(&vec![0; usize::try_from(end - start).unwrap()])
        .unwrap();
}

#[test]
fn retention_interior_hole_refuses_without_truncation() {
    let fixture = Fixture::new();
    fixture.commit();
    fixture.erase();
    zero_range(&fixture.path, fixture.frames[4].0, fixture.frames[5].0);
    let before = std::fs::read(&fixture.path).unwrap();
    assert!(Scanner::inspect(&fixture.path).is_err());
    assert!(Writer::open(&fixture.path).is_err());
    assert!(
        Writer::open_verified(&fixture.path, Some(&fixture.binding(1)), &mut |_, _| {}).is_err()
    );
    assert_eq!(std::fs::read(&fixture.path).unwrap(), before);
}

#[test]
fn retention_shortened_committed_window_refuses_every_writer_without_truncation() {
    let fixture = Fixture::new();
    fixture.commit();
    let tail = fixture.binding(7);
    let activation = fixture.binding(1);
    let checkpoint = fixture.binding(5);
    for cut in [
        fixture.authority.boundary.offset,
        fixture.frames[6].0,
        tail.physical_tail - 2,
    ] {
        std::fs::write(
            &fixture.path,
            &fixture.bytes[..usize::try_from(cut).unwrap()],
        )
        .unwrap();
        let before = std::fs::read(&fixture.path).unwrap();
        assert!(Scanner::inspect(&fixture.path).is_err());
        assert!(Scanner::verify(&fixture.path).is_err());
        assert!(Scanner::walk_prefix(&tail, &mut |_, _| {}).is_err());
        assert!(Reader::replay(&fixture.path).is_err());
        assert!(Reader::tail(&fixture.path, Duration::from_millis(1)).is_err());
        assert!(Writer::open(&fixture.path).is_err());
        assert!(Writer::open_verified(&fixture.path, Some(&activation), &mut |_, _| {}).is_err());
        assert!(Writer::open_with_expected_tail(&fixture.path, &tail).is_err());
        let mut digest = blake3::Hasher::new();
        assert!(
            Writer::open_verified_checkpoint(
                &fixture.path,
                &activation,
                Some((&checkpoint, "ignored")),
                CheckpointPrefix::Defer,
                &mut digest,
                &mut |_| {},
                &mut |_, _| {}
            )
            .is_err()
        );
        assert!(
            Scanner::walk_bounded(
                &fixture.path,
                tail.physical_tail,
                &activation,
                None,
                &mut digest,
                &mut |_, _| {}
            )
            .is_err()
        );
        // Preparation bounds its walk by the current file length; a shortened log must still refuse.
        assert!(
            Scanner::walk_bounded(
                &fixture.path,
                cut,
                &activation,
                None,
                &mut blake3::Hasher::new(),
                &mut |_, _| {}
            )
            .is_err()
        );
        assert!(Scanner::verify_prefix(&fixture.binding(4)).is_err());
        assert_eq!(std::fs::read(&fixture.path).unwrap(), before);
    }
}

#[test]
fn retention_interior_bound_below_retained_tail_walks_an_intact_log() {
    let fixture = Fixture::new();
    fixture.commit();
    fixture.erase();
    let activation = fixture.binding(1);
    let interior = fixture.binding(4);
    let mut digest = blake3::Hasher::new();
    let walked = Scanner::walk_bounded(
        &fixture.path,
        interior.physical_tail,
        &activation,
        None,
        &mut digest,
        &mut |_, _| {},
    )
    .unwrap();
    assert_eq!(walked, interior);
    // `verify_prefix` checks the interior prefix and the complete suffix, returning the full tail.
    assert_eq!(
        Scanner::verify_prefix(&interior).unwrap(),
        fixture.binding(7)
    );
}

#[test]
fn retention_retained_tail_sequence_hash_and_offset_must_match() {
    let mut fixture = Fixture::new();
    let tail = fixture.binding(7);
    for field in 0..3 {
        fixture.authority.retained_tail = TailBinding::from(&tail);
        match field {
            0 => fixture.authority.retained_tail.last_hash = blake3::hash(b"wrong tail"),
            1 => fixture.authority.retained_tail.last_sequence = Some(EventSeq(6)),
            _ => fixture.authority.retained_tail.physical_tail -= 1,
        }
        fixture.commit();
        let before = std::fs::read(&fixture.path).unwrap();
        assert!(Scanner::inspect(&fixture.path).is_err());
        assert!(Scanner::walk_prefix(&tail, &mut |_, _| {}).is_err());
        assert!(Writer::open(&fixture.path).is_err());
        let mut digest = blake3::Hasher::new();
        assert!(
            Scanner::walk_bounded(
                &fixture.path,
                tail.physical_tail,
                &tail,
                None,
                &mut digest,
                &mut |_, _| {}
            )
            .is_err()
        );
        assert_eq!(std::fs::read(&fixture.path).unwrap(), before);
    }
}

#[test]
fn retention_resume_at_boundary_authenticates_pin_and_starts_empty_digest() {
    let fixture = Fixture::new();
    fixture.commit();
    fixture.erase();
    let boundary = fixture.binding(2);
    let tail = fixture.binding(7);
    let mut digest = Scanner::hash_prefix(&fixture.path, boundary.physical_tail).unwrap();
    assert_eq!(digest.finalize(), blake3::hash(b""));
    assert_eq!(
        Scanner::walk_bounded(
            &fixture.path,
            tail.physical_tail,
            &fixture.binding(1),
            Some(&boundary),
            &mut digest,
            &mut |_, _| {}
        )
        .unwrap(),
        tail
    );
    assert_eq!(
        digest.finalize(),
        Scanner::hash_prefix(&fixture.path, tail.physical_tail)
            .unwrap()
            .finalize()
    );
    zero_range(&fixture.path, fixture.frames[2].0, fixture.frames[3].0);
    assert!(
        Scanner::walk_bounded(
            &fixture.path,
            tail.physical_tail,
            &tail,
            Some(&boundary),
            &mut digest,
            &mut |_, _| {}
        )
        .is_err()
    );
}

#[test]
fn retention_incomplete_append_after_retained_tail_is_repaired() {
    let fixture = Fixture::new();
    fixture.commit();
    fixture.erase();
    let original = std::fs::read(&fixture.path).unwrap();
    for mode in 0..3 {
        let mut bytes = original.clone();
        bytes.extend_from_slice(&[10, 0]);
        std::fs::write(&fixture.path, bytes).unwrap();
        assert!(
            Scanner::inspect(&fixture.path)
                .unwrap()
                .incomplete_tail
                .is_some()
        );
        let mut digest = blake3::Hasher::new();
        let writer = match mode {
            0 => Writer::open(&fixture.path).unwrap(),
            1 => {
                Writer::open_verified(&fixture.path, Some(&fixture.binding(1)), &mut |_, _| {})
                    .unwrap()
                    .0
            }
            _ => {
                Writer::open_verified_checkpoint(
                    &fixture.path,
                    &fixture.binding(1),
                    None,
                    CheckpointPrefix::Verify,
                    &mut digest,
                    &mut |_| {},
                    &mut |_, _| {},
                )
                .unwrap()
                .0
            }
        };
        drop(writer);
        assert_eq!(std::fs::read(&fixture.path).unwrap(), original);
    }
}

#[test]
fn retention_moved_capture_uses_pathless_retained_tail() {
    let fixture = Fixture::new();
    fixture.commit();
    fixture.erase();
    let moved = fixture.path.with_file_name("moved.log");
    std::fs::rename(&fixture.path, &moved).unwrap();
    std::fs::rename(retention_path(&fixture.path), retention_path(&moved)).unwrap();
    let tail = Scanner::verify(&moved).unwrap();
    assert_eq!(
        tail,
        fixture.authority.retained_tail.resolve(&moved).unwrap()
    );
    drop(Writer::open(&moved).unwrap());
}

#[test]
fn retention_missing_authority_beside_erased_log_refuses() {
    let fixture = Fixture::new();
    fixture.erase();
    assert!(Scanner::verify(&fixture.path).is_err());
    let before = std::fs::read(&fixture.path).unwrap();
    assert!(Writer::open(&fixture.path).is_err());
    assert_eq!(std::fs::read(&fixture.path).unwrap(), before);
}

#[test]
fn retention_feed_write_iterate_verify_and_uncommitted_invisibility() {
    let mut fixture = Fixture::new();
    fixture.archive();
    let expected = [0, 2]
        .into_iter()
        .flat_map(|index| {
            fixture.bytes[usize::try_from(fixture.frames[index].0).unwrap()
                ..usize::try_from(fixture.frames[index + 1].0).unwrap()]
                .to_vec()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        std::fs::read(feed_path(&fixture.path, 1)).unwrap(),
        expected
    );
    let mut uncommitted = FeedArchiveWriter::new(&fixture.path, 2).unwrap();
    let frame = &fixture.frames[3];
    uncommitted
        .append(frame.0, frame.1, frame.2.prev_hash)
        .unwrap();
    uncommitted.finish().unwrap();
    assert!(
        FeedArchiveIter::open(
            &fixture.path,
            &RetentionAuthority {
                feed: vec![],
                ..fixture.authority.clone()
            },
            &fixture.receipts
        )
        .unwrap()
        .next()
        .is_none()
    );
    fixture.commit();
    fixture.erase();
    assert_eq!(
        fixture.feeds().unwrap(),
        vec![fixture.frames[0].2.clone(), fixture.frames[2].2.clone()]
    );
    assert!(FeedArchiveWriter::new(&fixture.path, 1).is_err());
    // A failed epoch is overwritten on retry, never exposed by directory enumeration.
    let mut retry = FeedArchiveWriter::new(&fixture.path, 2).unwrap();
    let frame = &fixture.frames[4];
    retry.append(frame.0, frame.1, frame.2.prev_hash).unwrap();
    let entry = retry.finish().unwrap().unwrap();
    fixture.authority.epoch = 2;
    fixture.authority.feed.push(entry);
    fixture.commit();
    assert_eq!(
        fixture
            .feeds()
            .unwrap()
            .iter()
            .map(|frame| frame.seq)
            .collect::<Vec<_>>(),
        vec![EventSeq(0), EventSeq(2), EventSeq(4)]
    );
}

#[test]
fn retention_feed_empty_epoch_writes_no_file_or_entry() {
    let fixture = Fixture::new();
    let mut failed = FeedArchiveWriter::new(&fixture.path, 1).unwrap();
    failed
        .append(
            fixture.frames[0].0,
            EventSeq(0),
            fixture.frames[0].2.prev_hash,
        )
        .unwrap();
    failed.finish().unwrap();
    assert!(
        FeedArchiveWriter::new(&fixture.path, 1)
            .unwrap()
            .finish()
            .unwrap()
            .is_none()
    );
    assert!(!feed_path(&fixture.path, 1).exists());
}

#[test]
fn retention_feed_refuses_missing_short_dropped_repeated_and_tampered_files() {
    let mut fixture = Fixture::new();
    fixture.archive();
    fixture.commit();
    let path = feed_path(&fixture.path, 1);
    let bytes = std::fs::read(&path).unwrap();
    let first_end = usize::try_from(fixture.frames[1].0 - fixture.frames[0].0).unwrap();
    std::fs::remove_file(&path).unwrap();
    assert!(fixture.feeds().is_err());
    let repeated = [
        bytes[..first_end].to_vec(),
        bytes[..first_end].to_vec(),
        bytes[first_end..].to_vec(),
    ]
    .concat();
    for bad in [
        bytes[..first_end].to_vec(),
        bytes[first_end..].to_vec(),
        repeated,
        bytes[..bytes.len() - 1].to_vec(),
    ] {
        std::fs::write(&path, bad).unwrap();
        assert!(fixture.feeds().is_err());
    }
    std::fs::write(&path, &bytes).unwrap();
    for field in 0..3 {
        let mut authority = fixture.authority.clone();
        match field {
            0 => authority.feed[0].frame_count += 1,
            1 => authority.feed[0].length += 1,
            _ => authority.feed[0].hash = blake3::hash(b"bad digest"),
        }
        assert!(
            FeedArchiveIter::open(&fixture.path, &authority, &fixture.receipts)
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .is_err()
        );
    }
    // Even matching file commitments cannot permit a repeated or reordered receipt.
    for bad in [
        [bytes[..first_end].to_vec(), bytes[..first_end].to_vec()].concat(),
        [bytes[first_end..].to_vec(), bytes[..first_end].to_vec()].concat(),
    ] {
        std::fs::write(&path, &bad).unwrap();
        let mut authority = fixture.authority.clone();
        authority.feed[0].length = u64::try_from(bad.len()).unwrap();
        authority.feed[0].hash = blake3::hash(&bad);
        assert!(
            FeedArchiveIter::open(&fixture.path, &authority, &fixture.receipts)
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .is_err()
        );
    }
}

#[test]
fn retention_feed_refuses_receipt_mismatch_and_record_corruption() {
    let mut fixture = Fixture::new();
    fixture.archive();
    fixture.commit();
    let bytes = std::fs::read(&fixture.receipts).unwrap();
    for field in 0..3 {
        let mut wrong = ReceiptRecord {
            receipt: AppendReceipt {
                sequence: EventSeq(0),
                this_hash: fixture.frames[0].2.this_hash,
            },
            received_millis: 0,
            byte_offset: Some(fixture.frames[0].0),
        };
        match field {
            0 => wrong.receipt.this_hash = blake3::hash(b"wrong receipt"),
            1 => wrong.received_millis = 100,
            _ => {}
        }
        let mut bad = bytes.clone();
        bad[..80].copy_from_slice(&wrong.encode());
        if field == 2 {
            bad[79] ^= 1;
        }
        std::fs::write(&fixture.receipts, bad).unwrap();
        assert!(fixture.feeds().is_err());
    }
}

#[test]
fn retention_receipt_codec_preserves_layout_and_sentinel() {
    for offset in [Some(55), None] {
        let record = ReceiptRecord {
            receipt: AppendReceipt {
                sequence: EventSeq(7),
                this_hash: blake3::hash(b"receipt"),
            },
            received_millis: -42,
            byte_offset: offset,
        };
        let bytes = record.encode();
        assert_eq!(&bytes[..32], record.receipt.this_hash.as_bytes());
        assert_eq!(&bytes[32..40], &(-42i64).to_le_bytes());
        assert_eq!(&bytes[40..48], &offset.unwrap_or(u64::MAX).to_le_bytes());
        assert_eq!(&bytes[48..], blake3::hash(&bytes[..48]).as_bytes());
        assert_eq!(
            ReceiptRecord::read(&mut bytes.as_slice(), EventSeq(7)).unwrap(),
            record
        );
        let mut corrupt = bytes;
        corrupt[32] ^= 1;
        assert!(ReceiptRecord::read(&mut corrupt.as_slice(), EventSeq(7)).is_err());
        assert!(ReceiptRecord::read(&mut &bytes[..79], EventSeq(7)).is_err());
    }
}
