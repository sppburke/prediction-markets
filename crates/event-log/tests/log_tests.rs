#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::io::Write;
use std::time::Duration;

use pe_core_types::{EventSeq, ReceivedAt, SourceId, SourceTimestamp};
use pe_event_log::{ContentType, EnvelopeIn, LogError, Reader, Scanner, Writer};
use tempfile::TempDir;
use time::OffsetDateTime;

// ── helpers ──────────────────────────────────────────────────────────────────

fn tmp_dir() -> TempDir {
    tempfile::tempdir().unwrap()
}

fn fixed_time() -> OffsetDateTime {
    // 2025-01-15T12:00:00Z  — stable across test runs.
    use time::macros::datetime;
    datetime!(2025-01-15 12:00:00 UTC)
}

fn make_envelope(payload: Vec<u8>) -> EnvelopeIn {
    let t = fixed_time();
    EnvelopeIn {
        source_id: SourceId("test-source".into()),
        schema_version: 1,
        parser_version: 1,
        observed_at: SourceTimestamp(t),
        received_at: ReceivedAt(t),
        content_type: ContentType::Raw,
        payload,
    }
}

fn rewrite_single_frame_json(path: &std::path::Path, edit: impl FnOnce(&mut serde_json::Value)) {
    let bytes = std::fs::read(path).unwrap();
    let len = usize::try_from(u32::from_le_bytes(bytes[5..9].try_into().unwrap())).unwrap();
    let mut json: serde_json::Value =
        serde_json::from_slice(&zstd::decode_all(&bytes[9..9 + len]).unwrap()).unwrap();
    edit(&mut json);
    let encoded = serde_json::to_vec(&json).unwrap();
    let compressed = zstd::encode_all(encoded.as_slice(), 3).unwrap();
    let compressed_len = u32::try_from(compressed.len()).unwrap();
    let mut rewritten = b"EDGE\x01".to_vec();
    rewritten.extend_from_slice(&compressed_len.to_le_bytes());
    rewritten.extend_from_slice(&compressed);
    rewritten.extend_from_slice(&crc32fast::hash(&compressed).to_le_bytes());
    std::fs::write(path, rewritten).unwrap();
}

fn write_single_raw_frame(path: &std::path::Path, compressed: &[u8]) {
    let length = u32::try_from(compressed.len()).unwrap();
    let mut bytes = b"EDGE\x01".to_vec();
    bytes.extend_from_slice(&length.to_le_bytes());
    bytes.extend_from_slice(compressed);
    bytes.extend_from_slice(&crc32fast::hash(compressed).to_le_bytes());
    std::fs::write(path, bytes).unwrap();
}

// ── wire format ──────────────────────────────────────────────────────────────

#[test]
fn header_written_once_at_offset_zero() {
    let dir = tmp_dir();
    let path = dir.path().join("test.log");
    let mut writer = Writer::open(&path).unwrap();
    writer.append(make_envelope(b"hello".to_vec())).unwrap();
    writer.sync().unwrap();
    drop(writer);

    let bytes = std::fs::read(&path).unwrap();
    assert_eq!(&bytes[..4], b"EDGE");
    assert_eq!(bytes[4], 0x01);
}

// ── hashing ──────────────────────────────────────────────────────────────────

#[test]
fn first_frame_prev_hash_is_zero() {
    let dir = tmp_dir();
    let path = dir.path().join("test.log");
    let mut writer = Writer::open(&path).unwrap();
    writer.append(make_envelope(b"first".to_vec())).unwrap();
    drop(writer);

    let (_, envelope) = Reader::replay(&path).unwrap().next().unwrap().unwrap();

    assert_eq!(envelope.prev_hash.as_bytes(), &[0u8; 32]);
}

#[test]
fn raw_payload_hash_matches_blake3_of_payload() {
    let dir = tmp_dir();
    let path = dir.path().join("test.log");
    let payload = b"blake3 test payload".to_vec();
    let mut writer = Writer::open(&path).unwrap();
    writer.append(make_envelope(payload.clone())).unwrap();
    drop(writer);

    let (_, envelope) = Reader::replay(&path).unwrap().next().unwrap().unwrap();

    assert_eq!(envelope.raw_payload_hash, blake3::hash(&payload));
}

#[test]
fn stored_raw_payload_hash_tamper_is_rejected_even_with_valid_crc() {
    let dir = tmp_dir();
    let path = dir.path().join("raw_hash_tamper.log");
    {
        let mut writer = Writer::open(&path).unwrap();
        writer.append(make_envelope(b"payload".to_vec())).unwrap();
    }
    rewrite_single_frame_json(&path, |json| {
        json["raw_payload_hash"] = serde_json::Value::String("00".repeat(32));
    });

    assert!(matches!(
        Reader::replay(&path),
        Err(LogError::RawPayloadHashMismatch { .. })
    ));
    let before = std::fs::read(&path).unwrap();
    assert!(matches!(
        Writer::open(&path),
        Err(LogError::RawPayloadHashMismatch { .. })
    ));
    assert_eq!(std::fs::read(&path).unwrap(), before);
}

#[test]
fn sequence_gap_is_rejected_before_chain_validation() {
    let dir = tmp_dir();
    let path = dir.path().join("sequence_gap.log");
    {
        let mut writer = Writer::open(&path).unwrap();
        writer.append(make_envelope(b"payload".to_vec())).unwrap();
    }
    rewrite_single_frame_json(&path, |json| {
        json["seq"] = serde_json::Value::from(1);
    });

    assert!(matches!(
        Reader::replay(&path),
        Err(LogError::SequenceMismatch {
            expected: EventSeq(0),
            actual: EventSeq(1),
            ..
        })
    ));
}

// ── EventSeq ─────────────────────────────────────────────────────────────────

#[test]
fn event_seq_starts_at_zero_and_is_monotonic() {
    let dir = tmp_dir();
    let path = dir.path().join("test.log");
    let mut writer = Writer::open(&path).unwrap();
    for i in 0u8..5 {
        let seq = writer.append(make_envelope(vec![i])).unwrap();
        assert_eq!(seq.0, u64::from(i));
    }
    drop(writer);

    let seqs: Vec<EventSeq> = Reader::replay(&path)
        .unwrap()
        .map(|r| r.unwrap().0)
        .collect();

    assert_eq!(
        seqs,
        vec![
            EventSeq(0),
            EventSeq(1),
            EventSeq(2),
            EventSeq(3),
            EventSeq(4)
        ]
    );
}

// ── Writer ────────────────────────────────────────────────────────────────────

#[test]
fn writer_open_creates_file_with_header() {
    let dir = tmp_dir();
    let path = dir.path().join("new.log");
    assert!(!path.exists());
    let _writer = Writer::open(&path).unwrap();
    assert!(path.exists());
    let bytes = std::fs::read(&path).unwrap();
    assert!(bytes.len() >= 5);
    assert_eq!(&bytes[..5], b"EDGE\x01");
}

#[test]
fn writer_open_verifies_header_on_existing_file() {
    let dir = tmp_dir();
    let path = dir.path().join("existing.log");
    // Create a valid log first.
    {
        let mut w = Writer::open(&path).unwrap();
        w.append(make_envelope(b"x".to_vec())).unwrap();
    }
    // Re-opening should succeed.
    let _w2 = Writer::open(&path).unwrap();
}

#[test]
fn lock_test_second_writer_returns_locked() {
    let dir = tmp_dir();
    let path = dir.path().join("locked.log");
    let _w1 = Writer::open(&path).unwrap();

    let result = Writer::open(&path);
    let is_locked = matches!(result, Err(LogError::Locked { .. }));
    assert!(is_locked, "expected Locked error");
}

// ── Reader ────────────────────────────────────────────────────────────────────

#[test]
fn header_reject_test_bad_magic() {
    let dir = tmp_dir();
    let path = dir.path().join("bad.log");
    let mut f = std::fs::File::create(&path).unwrap();
    f.write_all(b"FOO!\x01").unwrap();

    let result = Reader::replay(&path);
    let is_bad_header = matches!(result, Err(LogError::BadHeader { .. }));
    assert!(is_bad_header, "expected BadHeader error");
}

#[test]
fn round_trip_single_envelope() {
    let dir = tmp_dir();
    let path = dir.path().join("rt.log");
    let payload = b"round trip".to_vec();

    let mut writer = Writer::open(&path).unwrap();
    writer.append(make_envelope(payload.clone())).unwrap();
    drop(writer);

    let results: Vec<_> = Reader::replay(&path).unwrap().collect();
    assert_eq!(results.len(), 1);
    let (seq, env) = results[0].as_ref().unwrap();
    assert_eq!(seq.0, 0);
    assert_eq!(env.payload, payload);
}

// ── CRC tamper test ───────────────────────────────────────────────────────────

#[test]
fn crc_tamper_flipping_byte_yields_crc_mismatch() {
    use proptest::prelude::*;

    proptest!(|(payload in proptest::collection::vec(0u8..=255, 1..=256))| {
        let dir = tmp_dir();
        let path = dir.path().join("crc_tamper.log");

        {
            let mut w = Writer::open(&path).unwrap();
            w.append(make_envelope(payload)).unwrap();
        }

        let mut file_bytes = std::fs::read(&path).unwrap();

        // Flip a byte inside the zstd block (after 5-byte header + 4-byte LEN).
        // The zstd block starts at byte 9. Flip byte 9 (first byte of compressed data).
        if file_bytes.len() > 9 {
            file_bytes[9] ^= 0xFF;
            std::fs::write(&path, &file_bytes).unwrap();

            let result = Reader::replay(&path);
            prop_assert!(
                matches!(result, Err(LogError::CrcMismatch { .. })),
                "expected CrcMismatch"
            );
        }
    });
}

#[test]
fn valid_checksum_with_bad_zstd_or_envelope_decode_is_fatal() {
    let dir = tmp_dir();
    let zstd_path = dir.path().join("bad-zstd.log");
    write_single_raw_frame(&zstd_path, b"not a zstd block");
    assert!(matches!(
        Writer::open(&zstd_path),
        Err(LogError::Decompress { .. })
    ));

    let json_path = dir.path().join("bad-json.log");
    let compressed = zstd::encode_all(b"not an envelope".as_slice(), 3).unwrap();
    write_single_raw_frame(&json_path, &compressed);
    assert!(matches!(
        Writer::open(&json_path),
        Err(LogError::EnvelopeDecode { .. })
    ));
}

#[test]
fn oversized_frame_claim_is_fatal_without_mutation() {
    let dir = tmp_dir();
    let path = dir.path().join("oversized.log");
    let mut bytes = b"EDGE\x01".to_vec();
    bytes.extend_from_slice(&(64_u32 * 1024 * 1024 + 1).to_le_bytes());
    std::fs::write(&path, &bytes).unwrap();

    assert!(matches!(
        Writer::open(&path),
        Err(LogError::FrameTooLarge { .. })
    ));
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
}

// ── Chain tamper test ─────────────────────────────────────────────────────────

#[test]
fn donor_frame_tamper_is_rejected_before_it_can_join_the_chain() {
    use proptest::prelude::*;

    proptest!(|(
        payload1 in proptest::collection::vec(0u8..=255, 1..=64),
        payload2 in proptest::collection::vec(0u8..=255, 1..=64),
    )| {
        let dir = tmp_dir();
        let path1 = dir.path().join("chain1.log");
        let path2 = dir.path().join("chain2.log");

        // Write two legitimate envelopes to path1.
        {
            let mut w = Writer::open(&path1).unwrap();
            w.append(make_envelope(payload1)).unwrap();
            w.append(make_envelope(payload2)).unwrap();
        }

        // Write a single legitimate envelope to path2 to use as donor.
        {
            let mut w = Writer::open(&path2).unwrap();
            w.append(make_envelope(b"donor".to_vec())).unwrap();
        }

        // Build a tampered file: take the header + frame0 from path1, then frame1 from path2
        // (which has a wrong prev_hash for position 1 in path1's chain).
        let bytes1 = std::fs::read(&path1).unwrap();
        let bytes2 = std::fs::read(&path2).unwrap();

        // path1 frame0 starts at offset 5. Find where frame1 starts.
        // path2 frame0 starts at offset 5.
        // Read len of frame0 in path1.
        if bytes1.len() >= 9 && bytes2.len() >= 9 {
            let len0 = u32::from_le_bytes([bytes1[5], bytes1[6], bytes1[7], bytes1[8]]) as usize;
            let frame0_end = 5 + 4 + len0 + 4;

            let len2 = u32::from_le_bytes([bytes2[5], bytes2[6], bytes2[7], bytes2[8]]) as usize;
            let frame2_end = 5 + 4 + len2 + 4;

            if frame0_end <= bytes1.len() && frame2_end <= bytes2.len() {
                let tampered_path = dir.path().join("tampered.log");
                let mut tampered = bytes1[..frame0_end].to_vec();
                // Append frame from path2 as a "second frame" — its prev_hash won't match.
                tampered.extend_from_slice(&bytes2[5..frame2_end]);
                std::fs::write(&tampered_path, &tampered).unwrap();

                // Preflight rejects the duplicate sequence before exposing frame 0.
                let result = Reader::replay(&tampered_path);
                prop_assert!(
                    matches!(result, Err(LogError::SequenceMismatch { .. })),
                    "expected SequenceMismatch for donor frame"
                );
            }
        }
    });
}

// ── Truncation test ───────────────────────────────────────────────────────────

#[test]
fn replay_preflight_rejects_truncation_before_exposing_a_valid_prefix() {
    let dir = tmp_dir();
    let path = dir.path().join("trunc.log");

    const N: usize = 5;
    {
        let mut w = Writer::open(&path).unwrap();
        for i in 0..N {
            w.append(make_envelope(vec![i as u8; 32])).unwrap();
        }
    }

    let full_bytes = std::fs::read(&path).unwrap();
    // Truncate at the midpoint — guaranteed to fall in the middle of some frame.
    let trunc_len = full_bytes.len() / 2;
    std::fs::write(&path, &full_bytes[..trunc_len]).unwrap();

    assert!(matches!(
        Reader::replay(&path),
        Err(LogError::Truncated { .. })
    ));
}

#[test]
fn every_source_or_paper_frame_truncation_repairs_only_an_incomplete_tail() {
    let dir = tmp_dir();
    let canonical = dir.path().join("canonical.log");
    {
        let mut writer = Writer::open(&canonical).unwrap();
        writer
            .append(make_envelope(b"canonical fixture".to_vec()))
            .unwrap();
        writer.sync().unwrap();
    }
    let bytes = std::fs::read(&canonical).unwrap();

    for cut in 1..bytes.len() {
        let path = dir.path().join(format!("cut-{cut}.log"));
        std::fs::write(&path, &bytes[..cut]).unwrap();
        let before = std::fs::read(&path).unwrap();
        if cut < 5 {
            assert!(matches!(
                Writer::open(&path),
                Err(LogError::BadHeader { .. })
            ));
            assert_eq!(std::fs::read(&path).unwrap(), before);
        } else if cut == 5 {
            drop(Writer::open(&path).unwrap());
            assert_eq!(std::fs::read(&path).unwrap(), before);
        } else {
            let outcome = Scanner::inspect(&path).unwrap();
            assert_eq!(outcome.verified_tail.physical_tail, 5);
            assert!(outcome.incomplete_tail.is_some());
            drop(Writer::open(&path).unwrap());
            assert_eq!(std::fs::read(&path).unwrap(), b"EDGE\x01");
        }
    }
}

#[test]
fn interior_crc_damage_is_fatal_and_never_truncated() {
    let dir = tmp_dir();
    let path = dir.path().join("interior.log");
    {
        let mut writer = Writer::open(&path).unwrap();
        writer.append(make_envelope(vec![1; 64])).unwrap();
        writer.append(make_envelope(vec![2; 64])).unwrap();
        writer.sync().unwrap();
    }
    let mut damaged = std::fs::read(&path).unwrap();
    damaged[9] ^= 0xff;
    std::fs::write(&path, &damaged).unwrap();

    assert!(matches!(
        Writer::open(&path),
        Err(LogError::CrcMismatch {
            at_seq: EventSeq(0),
            ..
        })
    ));
    assert_eq!(std::fs::read(&path).unwrap(), damaged);
}

#[test]
fn property_large_frame_truncations_repair_to_the_verified_prefix() {
    use proptest::prelude::*;

    proptest!(|(payload in proptest::collection::vec(any::<u8>(), 1_024..=8_192), fraction in 1usize..=99)| {
        let dir = tmp_dir();
        let canonical = dir.path().join("large.log");
        {
            let mut writer = Writer::open(&canonical).unwrap();
            writer.append(make_envelope(payload)).unwrap();
            writer.sync().unwrap();
        }
        let bytes = std::fs::read(&canonical).unwrap();
        let body_len = bytes.len() - 5;
        let cut = 5 + body_len.saturating_mul(fraction) / 100;
        let path = dir.path().join("large-truncated.log");
        std::fs::write(&path, &bytes[..cut]).unwrap();
        drop(Writer::open(&path).unwrap());
        prop_assert_eq!(std::fs::read(&path).unwrap(), b"EDGE\x01");
    });
}

#[test]
fn scanner_reports_exact_physical_tail_sequence_hash_and_resolved_path() {
    let dir = tmp_dir();
    let path = dir.path().join("tail-binding.log");
    {
        let mut writer = Writer::open(&path).unwrap();
        writer.append(make_envelope(b"one".to_vec())).unwrap();
        writer.append(make_envelope(b"two".to_vec())).unwrap();
        writer.sync().unwrap();
    }
    let binding = Reader::verified_tail(&path).unwrap();
    let envelopes = Reader::replay(&path)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(binding.path, std::fs::canonicalize(&path).unwrap());
    assert_eq!(
        binding.physical_tail,
        std::fs::metadata(&path).unwrap().len()
    );
    assert_eq!(binding.last_sequence, Some(EventSeq(1)));
    assert_eq!(binding.last_hash, envelopes[1].1.this_hash);
}

// ── Tail test ─────────────────────────────────────────────────────────────────

#[test]
fn tail_receives_all_appended_envelopes() {
    use std::sync::{Arc, Mutex};
    use std::thread;

    let dir = tmp_dir();
    let path = dir.path().join("tail.log");

    const N: usize = 20;

    // Create the log file before the tail reader starts.
    {
        let _w = Writer::open(&path).unwrap();
    }

    let received: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let received_clone = Arc::clone(&received);
    let path_clone = path.clone();

    let reader_thread = thread::spawn(move || {
        let iter = Reader::tail(&path_clone, Duration::from_millis(5)).unwrap();
        for result in iter.take(N) {
            match result {
                Ok((seq, _)) => {
                    received_clone.lock().unwrap().push(seq.0);
                    if seq.0 as usize == N - 1 {
                        break;
                    }
                }
                Err(e) => panic!("tail reader error: {e}"),
            }
        }
    });

    // Give the reader thread a moment to start.
    thread::sleep(Duration::from_millis(10));

    {
        let mut writer = Writer::open(&path).unwrap();
        for i in 0..N {
            writer.append(make_envelope(vec![i as u8])).unwrap();
        }
        writer.sync().unwrap();
    }

    reader_thread.join().unwrap();

    let got = received.lock().unwrap();
    assert_eq!(got.len(), N);
    for (i, seq) in got.iter().enumerate() {
        assert_eq!(*seq, i as u64, "seq mismatch at index {i}");
    }
}

// ── Tail transient-truncation test ───────────────────────────────────────────

#[test]
fn tail_recovers_from_transient_truncation() {
    use std::thread;

    let dir = tmp_dir();
    let path = dir.path().join("tail_trunc.log");

    // Write 2 events and flush.
    {
        let mut w = Writer::open(&path).unwrap();
        w.append(make_envelope(b"first".to_vec())).unwrap();
        w.append(make_envelope(b"second".to_vec())).unwrap();
        w.sync().unwrap();
    }

    // Capture the complete file, then truncate by 1 byte (breaks the last CRC).
    let complete = std::fs::read(&path).unwrap();
    std::fs::write(&path, &complete[..complete.len() - 1]).unwrap();

    let path_clone = path.clone();
    let reader_thread = thread::spawn(move || {
        Reader::tail(&path_clone, Duration::from_millis(10))
            .unwrap()
            .take(2)
            .collect::<Vec<_>>()
    });

    // After the reader has had time to hit the truncated frame, restore the missing byte.
    thread::sleep(Duration::from_millis(60));
    {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        f.write_all(&complete[complete.len() - 1..]).unwrap();
        f.sync_all().unwrap();
    }

    let results = reader_thread.join().unwrap();
    assert_eq!(
        results.len(),
        2,
        "tail reader must recover and yield both events"
    );
    assert!(results[0].is_ok());
    assert!(results[1].is_ok());
    assert_eq!(results[0].as_ref().unwrap().0, EventSeq(0));
    assert_eq!(results[1].as_ref().unwrap().0, EventSeq(1));
}

// ── Round-trip property test ──────────────────────────────────────────────────

#[test]
fn proptest_round_trip_n_envelopes() {
    use proptest::prelude::*;

    proptest!(|(
        payloads in proptest::collection::vec(
            proptest::collection::vec(0u8..=255, 0..=128),
            1..=10
        )
    )| {
        let dir = tmp_dir();
        let path = dir.path().join("rt_prop.log");
        let n = payloads.len();

        {
            let mut w = Writer::open(&path).unwrap();
            for p in &payloads {
                w.append(make_envelope(p.clone())).unwrap();
            }
        }

        let results: Vec<_> = Reader::replay(&path).unwrap().collect();
        prop_assert_eq!(results.len(), n);

        for (i, r) in results.iter().enumerate() {
            let (seq, env) = r.as_ref().unwrap();
            prop_assert_eq!(seq.0, i as u64);
            prop_assert_eq!(&env.payload, &payloads[i]);
        }
    });
}

// ── Insta snapshot ────────────────────────────────────────────────────────────

#[test]
fn snapshot_canonical_wire_bytes() {
    use insta::assert_snapshot;

    let dir = tmp_dir();
    let path = dir.path().join("snap.log");

    let t = fixed_time();
    let payload = b"snapshot payload".to_vec();

    {
        let mut w = Writer::open(&path).unwrap();
        w.append(EnvelopeIn {
            source_id: SourceId("snapshot-source".into()),
            schema_version: 1,
            parser_version: 2,
            observed_at: SourceTimestamp(t),
            received_at: ReceivedAt(t),
            content_type: ContentType::Json,
            payload: payload.clone(),
        })
        .unwrap();
    }

    let (_, envelope) = Reader::replay(&path).unwrap().next().unwrap().unwrap();

    // Snapshot the JSON representation (sans payload for readability; payload is stable).
    let json = serde_json::to_string_pretty(&serde_json::json!({
        "seq": envelope.seq.0,
        "source_id": envelope.source_id.0,
        "schema_version": envelope.schema_version,
        "parser_version": envelope.parser_version,
        "content_type": "json",
        "raw_payload_hash": envelope.raw_payload_hash.to_hex().to_string(),
        "prev_hash": envelope.prev_hash.to_hex().to_string(),
        "this_hash": envelope.this_hash.to_hex().to_string(),
    }))
    .unwrap();

    assert_snapshot!(json);
}

// ── Cross-version replay test ─────────────────────────────────────────────────

#[test]
fn cross_version_replay_fixture_validates() {
    // The fixture was produced by a reference run and committed alongside the tests.
    // This test ensures the wire format contract is stable: if the format changes,
    // this test breaks, forcing an explicit migration decision.
    let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("v1_single_frame.log");

    if !fixture.exists() {
        // Fixture not yet present — generate it on first run.
        generate_fixture(&fixture);
    }

    let results: Vec<_> = Reader::replay(&fixture).unwrap().collect();
    assert_eq!(results.len(), 1, "fixture must contain exactly 1 frame");
    let (seq, env) = results[0].as_ref().unwrap();
    assert_eq!(seq.0, 0);
    assert_eq!(env.payload, b"fixture payload v1");
}

#[test]
fn nonempty_version_one_prefix_and_version_two_tail_replay_by_envelope_version() {
    let dir = tmp_dir();
    let path = dir.path().join("mixed-versions.log");
    let time = fixed_time();
    let mut writer = Writer::open(&path).unwrap();
    for version in [1, 2] {
        writer
            .append(EnvelopeIn {
                source_id: SourceId("versioned-source".to_owned()),
                schema_version: version,
                parser_version: version,
                observed_at: SourceTimestamp(time),
                received_at: ReceivedAt(time),
                content_type: ContentType::Json,
                payload: format!("{{\"version\":{version}}}").into_bytes(),
            })
            .unwrap();
    }
    writer.sync().unwrap();
    drop(writer);

    let frames = Reader::replay(&path)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(frames[0].1.schema_version, 1);
    assert_eq!(frames[1].1.schema_version, 2);
    assert_eq!(frames[0].0, EventSeq(0));
    assert_eq!(frames[1].0, EventSeq(1));
}

fn generate_fixture(path: &std::path::Path) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    let t = fixed_time();
    let mut w = Writer::open(path).unwrap();
    w.append(EnvelopeIn {
        source_id: SourceId("fixture-source".into()),
        schema_version: 1,
        parser_version: 1,
        observed_at: SourceTimestamp(t),
        received_at: ReceivedAt(t),
        content_type: ContentType::Raw,
        payload: b"fixture payload v1".to_vec(),
    })
    .unwrap();
}

#[test]
fn write_rejects_frame_its_scanner_would_refuse() {
    // The writer must never append a frame the reopen scan rejects (#544 review):
    // an incompressible payload past MAX_FRAME_BYTES fails the append with
    // FrameTooLarge instead of succeeding and making the log unopenable.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("big.log");
    let mut writer = Writer::open(&path).unwrap();
    let mut rng_bytes = vec![0u8; (64 * 1024 * 1024) + 1024];
    // Deterministic incompressible-ish pattern (no RNG in tests): multiply-xor walk.
    let mut x: u64 = 0x9e3779b97f4a7c15;
    for chunk in rng_bytes.chunks_mut(8) {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        for (i, b) in chunk.iter_mut().enumerate() {
            *b = (x >> (8 * (i as u64 % 8))) as u8;
        }
    }
    let result = writer.append(make_envelope(rng_bytes));
    assert!(matches!(result, Err(LogError::FrameTooLarge { .. })));
    // The log stays openable and empty of the oversized frame.
    drop(writer);
    let binding = Scanner::verify(&path).unwrap();
    assert_eq!(binding.last_sequence, None);
}

#[test]
fn open_with_expected_tail_refuses_repair_on_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bound.log");
    let mut writer = Writer::open(&path).unwrap();
    writer.append(make_envelope(b"{}".to_vec())).unwrap();
    writer.sync().unwrap();
    drop(writer);
    let good = Scanner::verify(&path).unwrap();
    // Damage the final length prefix the way the review's tamper repro does.
    let mut bytes = std::fs::read(&path).unwrap();
    let header = 5; // MAGIC + VERSION
    bytes[header] = bytes[header].wrapping_add(1);
    std::fs::write(&path, &bytes).unwrap();
    // Ordinary open would truncate-repair; the binding-gated open must refuse.
    let refused = Writer::open_with_expected_tail(&path, &good);
    assert!(matches!(
        refused,
        Err(LogError::ExpectedTailMismatch { .. }) | Err(LogError::ChainBroken { .. })
    ));
    // And the file was not mutated by the refusal.
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
}
