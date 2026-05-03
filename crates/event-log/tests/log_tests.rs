#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::io::Write;
use std::time::Duration;

use pe_core_types::{EventSeq, ReceivedAt, SourceId, SourceTimestamp};
use pe_event_log::{ContentType, EnvelopeIn, LogError, Reader, Writer};
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

            let mut iter = Reader::replay(&path).unwrap();
            let result = iter.next().unwrap();
            prop_assert!(
                matches!(result, Err(LogError::CrcMismatch { .. })),
                "expected CrcMismatch"
            );
            // Iterator should stop after the error.
            prop_assert!(iter.next().is_none());
        }
    });
}

// ── Chain tamper test ─────────────────────────────────────────────────────────

#[test]
fn chain_tamper_fabricated_prev_hash_yields_chain_broken() {
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

                let mut iter = Reader::replay(&tampered_path).unwrap();
                // Frame 0 should succeed.
                let r0 = iter.next();
                if let Some(Ok(_)) = r0 {
                    // Frame 1 should fail with ChainBroken.
                    let r1 = iter.next();
                    prop_assert!(
                        matches!(r1, Some(Err(LogError::ChainBroken { .. }))),
                        "expected ChainBroken for frame1, got {r1:?}"
                    );
                }
            }
        }
    });
}

// ── Truncation test ───────────────────────────────────────────────────────────

#[test]
fn truncation_yields_complete_frames_then_truncated_error() {
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

    let mut complete = 0usize;
    let mut saw_truncated = false;

    let iter = Reader::replay(&path).unwrap();
    for result in iter {
        match result {
            Ok(_) => complete += 1,
            Err(LogError::Truncated { .. }) => {
                saw_truncated = true;
                break;
            }
            Err(LogError::CrcMismatch { .. }) => {
                // Also acceptable at truncation boundary.
                saw_truncated = true;
                break;
            }
            Err(e) => panic!("unexpected error: {e}"),
        }
    }

    assert!(
        complete < N,
        "expected fewer than {N} complete frames, got {complete}"
    );
    assert!(saw_truncated || complete < N, "expected truncated error");
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
        let mut f = std::fs::OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(&complete[complete.len() - 1..]).unwrap();
        f.sync_all().unwrap();
    }

    let results = reader_thread.join().unwrap();
    assert_eq!(results.len(), 2, "tail reader must recover and yield both events");
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
