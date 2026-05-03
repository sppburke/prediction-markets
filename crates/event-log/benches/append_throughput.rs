#![allow(clippy::unwrap_used)]

use criterion::{Criterion, criterion_group, criterion_main};
use pe_core_types::{ReceivedAt, SourceId, SourceTimestamp};
use pe_event_log::{ContentType, EnvelopeIn, Writer};
use tempfile::TempDir;
use time::OffsetDateTime;

fn make_envelope_in(payload: Vec<u8>) -> EnvelopeIn {
    let now = OffsetDateTime::now_utc();
    EnvelopeIn {
        source_id: SourceId("bench-source".to_string()),
        schema_version: 1,
        parser_version: 1,
        observed_at: SourceTimestamp(now),
        received_at: ReceivedAt(now),
        content_type: ContentType::Raw,
        payload,
    }
}

fn bench_append(c: &mut Criterion) {
    let payload = vec![0u8; 1024];
    let dir = TempDir::new().unwrap();

    c.bench_function("append_1kb", |b| {
        b.iter_batched(
            || {
                let path = dir
                    .path()
                    .join(format!("bench_{}.log", uuid::Uuid::new_v4()));
                (Writer::open(&path).unwrap(), path)
            },
            |(mut writer, _path)| {
                writer.append(make_envelope_in(payload.clone())).unwrap();
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

criterion_group!(benches, bench_append);
criterion_main!(benches);
