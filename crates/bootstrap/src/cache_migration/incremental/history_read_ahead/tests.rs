#![expect(clippy::unwrap_used, reason = "a broken fixture must fail its test")]

use super::*;
use std::collections::BTreeMap;
use std::sync::Barrier;

#[derive(Default)]
struct Counts {
    live: BTreeMap<(usize, u8), usize>,
    peak: [usize; 5],
    bytes: usize,
    chunks: usize,
    blobs: usize,
    generations: usize,
    sample_rss: bool,
    rss_kib: Option<u64>,
    rows: Vec<(i64, bool)>,
}

fn resource_index(resource: Resource) -> u8 {
    match resource {
        Resource::Thread => 0,
        Resource::Connection => 1,
        Resource::Statement => 2,
        Resource::Blob => 3,
        Resource::Buffer => 4,
    }
}

impl Counts {
    fn observe(&mut self, event: HistoryReadAheadEvent) {
        match event {
            HistoryReadAheadEvent::Acquire { worker, resource } => {
                let index = resource_index(resource);
                let live = self.live.entry((worker, index)).or_default();
                *live += 1;
                assert!(*live <= if index == 3 { 2 } else { 1 });
                if index == 3 {
                    self.blobs += 1;
                }
                let total = self
                    .live
                    .iter()
                    .filter(|((_, kind), _)| *kind == index)
                    .map(|(_, count)| count)
                    .sum::<usize>();
                self.peak[usize::from(index)] = self.peak[usize::from(index)].max(total);
                assert!(total <= if index == 3 { 16 } else { 8 });
            }
            HistoryReadAheadEvent::Release { worker, resource } => {
                *self
                    .live
                    .get_mut(&(worker, resource_index(resource)))
                    .unwrap() -= 1;
            }
            HistoryReadAheadEvent::Point {
                point: Point::Chunk { bytes },
                ..
            } => {
                assert!(bytes <= HISTORY_READ_AHEAD_CHUNK_BYTES);
                self.bytes += bytes;
                self.chunks += 1;
                if self.sample_rss && self.chunks % 1024 == 1 {
                    self.rss_kib = rss_kib().map(|rss| self.rss_kib.unwrap_or(0).max(rss));
                }
            }
            HistoryReadAheadEvent::Point {
                point:
                    Point::Row {
                        rowid,
                        has_acquisition,
                    },
                ..
            } => {
                self.rows.push((rowid, has_acquisition));
            }
            HistoryReadAheadEvent::Point {
                point: Point::Generation { index, completed },
                ..
            } => {
                assert!(index < completed + HISTORY_READ_AHEAD_LEAD_GENERATIONS);
                self.generations += 1;
            }
            _ => {}
        }
    }

    fn released(&self) {
        assert!(self.live.values().all(|count| *count == 0));
    }
}

fn fixture(rows: usize, bytes: usize) -> (tempfile::TempDir, String, Connection) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("history.db").to_str().unwrap().to_owned();
    let connection = Connection::open(&path).unwrap();
    connection
        .execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;
         CREATE TABLE activity_wallet_coverage_staging_v2 (
             generation INTEGER, wallet_hex TEXT, page_evidence_json TEXT, acquisition_json TEXT,
             PRIMARY KEY(generation, wallet_hex));",
        )
        .unwrap();
    for row in 0..rows {
        connection.execute(
            "INSERT INTO activity_wallet_coverage_staging_v2 VALUES (1, ?1, zeroblob(?2), zeroblob(?2))",
            rusqlite::params![format!("{row:04}"), bytes],
        ).unwrap();
    }
    (dir, path, connection)
}

fn rss_kib() -> Option<u64> {
    std::fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))?
        .split_whitespace()
        .next()?
        .parse()
        .ok()
}

fn checkpoint(connection: &Connection) {
    let result: (i64, i64, i64) = connection
        .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .unwrap();
    assert_eq!(result, (0, 0, 0));
}

#[test]
fn history_read_ahead_budget_accounts_for_retained_overflow_cursors() {
    assert_eq!(budget(4096).unwrap(), 64_222_464);
    assert_eq!(budget(8192).unwrap(), 40_750_464);
    assert!(budget(0).is_err());
    assert!(budget(4095).is_err());
}

#[test]
fn history_read_ahead_large_columns_reuse_two_handles_and_one_chunk_buffer() {
    // Construction and verifier allocations are outside the helper measurement.
    let (_dir, path, connection) = fixture(3, 64 * 1024 * 1024);
    let baseline_rss = rss_kib();
    let counts = Arc::new(Mutex::new(Counts {
        sample_rss: true,
        ..Counts::default()
    }));
    let observed = Arc::clone(&counts);
    let first_chunk = Arc::new(Barrier::new(HISTORY_READ_AHEAD_CONNECTIONS));
    let both_columns = Arc::new(Barrier::new(HISTORY_READ_AHEAD_CONNECTIONS));
    let handles =
        std::array::from_fn::<_, HISTORY_READ_AHEAD_CONNECTIONS, _>(|_| AtomicUsize::new(0));
    let entered =
        std::array::from_fn::<_, HISTORY_READ_AHEAD_CONNECTIONS, _>(|_| AtomicBool::new(false));
    let progress = Progress::new(Hooks(Some(Arc::new(move |event| {
        observed.lock().unwrap().observe(event);
        if let HistoryReadAheadEvent::Acquire {
            worker,
            resource: Resource::Blob,
        } = event
            && handles[worker].fetch_add(1, Ordering::Relaxed) == 1
        {
            both_columns.wait();
        }
        if let HistoryReadAheadEvent::Point {
            worker,
            point: Point::Chunk { .. },
        } = event
            && !entered[worker].swap(true, Ordering::Relaxed)
        {
            first_chunk.wait();
        }
        HistoryReadAheadAction::Continue
    }))));
    let generations = [1; HISTORY_READ_AHEAD_CONNECTIONS];
    thread::scope(|scope| {
        let workers = (0..HISTORY_READ_AHEAD_CONNECTIONS)
            .map(|worker| {
                let path = &path;
                let progress = &progress;
                let generations = &generations;
                scope.spawn(move || warm(path, generations, progress, worker))
            })
            .collect::<Vec<_>>();
        for worker in workers {
            assert_eq!(worker.join().unwrap().status(), "done");
        }
    });
    let counts = counts.lock().unwrap();
    counts.released();
    assert_eq!(counts.peak, [8, 8, 8, 16, 8]);
    assert_eq!(
        counts.blobs, 16,
        "two handles per worker must reopen across all three rows"
    );
    assert_eq!(counts.bytes, 8 * 6 * 64 * 1024 * 1024);
    assert_eq!(counts.chunks, 8 * 6 * 1024);
    if let (Some(baseline), Some(peak)) = (baseline_rss, counts.rss_kib) {
        eprintln!(
            "isolated eight-reader RSS: baseline_kib={baseline}, sampled_peak_kib={peak}, delta_kib={}",
            peak.saturating_sub(baseline)
        );
    }
    checkpoint(&connection);
}

#[test]
fn history_read_ahead_eight_workers_obey_lead_and_cancel_while_waiting() {
    let (_dir, path, connection) = fixture(1, 1);
    let counts = Arc::new(Mutex::new(Counts::default()));
    let waiting = Arc::new((Mutex::new((false, 0_usize)), Condvar::new()));
    let opened = Arc::new(Barrier::new(HISTORY_READ_AHEAD_CONNECTIONS));
    let observed = Arc::clone(&counts);
    let parked = Arc::clone(&waiting);
    let progress = Progress::new(Hooks(Some(Arc::new(move |event| {
        observed.lock().unwrap().observe(event);
        match event {
            HistoryReadAheadEvent::Acquire {
                resource: Resource::Statement,
                ..
            } => {
                opened.wait();
            }
            HistoryReadAheadEvent::Point {
                point: Point::Waiting,
                ..
            } => {
                parked.0.lock().unwrap().0 = true;
                parked.1.notify_one();
            }
            HistoryReadAheadEvent::Point {
                point: Point::Generation { .. },
                ..
            } => {
                parked.0.lock().unwrap().1 += 1;
                parked.1.notify_one();
            }
            _ => {}
        }
        HistoryReadAheadAction::Continue
    }))));
    let generations = (1..=32).collect::<Vec<_>>();
    thread::scope(|scope| {
        let workers = (0..HISTORY_READ_AHEAD_CONNECTIONS)
            .map(|worker| {
                let path = &path;
                let generations = &generations;
                let progress = &progress;
                scope.spawn(move || warm(path, generations, progress, worker))
            })
            .collect::<Vec<_>>();
        let ready = waiting.0.lock().unwrap();
        let ready = waiting
            .1
            .wait_while(ready, |ready| !ready.0 || ready.1 < 8)
            .unwrap();
        drop(ready);
        progress.advance();
        let ready = waiting.0.lock().unwrap();
        let ready = waiting.1.wait_while(ready, |ready| ready.1 < 9).unwrap();
        drop(ready);
        progress.cancel();
        for worker in workers {
            assert_eq!(worker.join().unwrap().status(), "cancelled");
        }
    });
    let counts = counts.lock().unwrap();
    counts.released();
    assert_eq!(counts.peak[..3], [8, 8, 8]);
    assert_eq!(counts.peak[4], 8);
    assert_eq!(counts.generations, 9);
    checkpoint(&connection);
}

#[test]
fn history_read_ahead_cancellation_interrupts_sqlite_and_stops_between_chunks() {
    for sqlite in [false, true] {
        let (_dir, path, connection) = fixture(
            if sqlite { 1000 } else { 1 },
            2 * HISTORY_READ_AHEAD_CHUNK_BYTES,
        );
        let counts = Arc::new(Mutex::new(Counts::default()));
        let observed = Arc::clone(&counts);
        let stepping = Arc::new(AtomicBool::new(false));
        let fired = Arc::new(AtomicBool::new(false));
        let cancelled = Arc::clone(&fired);
        let progress = Progress::new(Hooks(Some(Arc::new(move |event| {
            observed.lock().unwrap().observe(event);
            if let HistoryReadAheadEvent::Point { point, .. } = event {
                if matches!(point, Point::Step) {
                    stepping.store(true, Ordering::Relaxed);
                }
                if (sqlite && matches!(point, Point::Progress) && stepping.load(Ordering::Relaxed))
                    || (!sqlite && matches!(point, Point::Chunk { .. }))
                {
                    cancelled.store(true, Ordering::Relaxed);
                    return HistoryReadAheadAction::Cancel;
                }
            }
            HistoryReadAheadAction::Continue
        }))));
        assert_eq!(warm(&path, &[1], &progress, 0).status(), "cancelled");
        assert!(fired.load(Ordering::Relaxed));
        let counts = counts.lock().unwrap();
        counts.released();
        if !sqlite {
            assert_eq!(counts.chunks, 1);
        }
        checkpoint(&connection);
    }
}

#[test]
fn history_read_ahead_unowned_interrupt_and_real_blob_busy_failures_are_unavailable() {
    let (_dir, path, connection) = fixture(1000, 1);
    let progress = Progress::new(Hooks(Some(Arc::new(|event| {
        if matches!(
            event,
            HistoryReadAheadEvent::Point {
                point: Point::Progress,
                ..
            }
        ) {
            return HistoryReadAheadAction::Fail(rusqlite::Error::InvalidQuery);
        }
        HistoryReadAheadAction::Continue
    }))));
    assert_eq!(warm(&path, &[1], &progress, 0).status(), "unavailable");
    assert!(!progress.stop.load(Ordering::Relaxed));
    connection
        .execute(
            "UPDATE activity_wallet_coverage_staging_v2 SET page_evidence_json = NULL",
            [],
        )
        .unwrap();
    assert_eq!(
        warm(&path, &[1], &Progress::new(Hooks::default()), 0).status(),
        "unavailable"
    );
    checkpoint(&connection);
    connection
        .execute_batch("PRAGMA journal_mode=DELETE; BEGIN EXCLUSIVE")
        .unwrap();
    assert_eq!(
        warm(&path, &[1], &Progress::new(Hooks::default()), 0).status(),
        "unavailable"
    );
    connection.execute_batch("ROLLBACK").unwrap();
}

#[test]
fn history_read_ahead_text_empty_and_null_columns_keep_index_order_and_reopen_safely() {
    let (_dir, path, connection) = fixture(0, 0);
    connection
        .execute_batch(
            "INSERT INTO activity_wallet_coverage_staging_v2 VALUES
         (1, 'c', 'third', 'last'), (1, 'a', '', NULL), (1, 'b', 'second', ''),
         (1, 'd', 'fourth', NULL), (1, 'e', 'fifth', 'again');",
        )
        .unwrap();
    let counts = Arc::new(Mutex::new(Counts::default()));
    let observed = Arc::clone(&counts);
    let progress = Progress::new(Hooks(Some(Arc::new(move |event| {
        observed.lock().unwrap().observe(event);
        HistoryReadAheadAction::Continue
    }))));
    assert_eq!(warm(&path, &[1], &progress, 0).status(), "done");
    let counts = counts.lock().unwrap();
    counts.released();
    assert_eq!(counts.blobs, 2);
    assert_eq!(counts.bytes, 31);
    assert_eq!(
        counts.rows,
        [(2, false), (3, true), (1, true), (4, false), (5, true)]
    );
    checkpoint(&connection);
}

#[test]
fn history_read_ahead_private_connection_settings_and_setup_fallbacks() {
    let (_dir, path, connection) = fixture(1, 1);
    let progress = Progress::new(Hooks::default());
    let reader = open(&path, &progress, 0, Arc::new(AtomicBool::new(false))).unwrap();
    assert_eq!(
        reader
            .pragma_query_value(None, "cache_size", |row| row.get::<_, i64>(0))
            .unwrap(),
        -2048
    );
    assert_eq!(
        reader
            .pragma_query_value(None, "busy_timeout", |row| row.get::<_, i64>(0))
            .unwrap(),
        50
    );
    let error = reader
        .execute("DELETE FROM activity_wallet_coverage_staging_v2", [])
        .unwrap_err();
    assert!(
        matches!(error, rusqlite::Error::SqliteFailure(error, _) if error.code == rusqlite::ErrorCode::ReadOnly)
    );
    drop(reader);
    let mut head = FreshCollectionIdentity::new(
        1,
        100,
        Vec::new(),
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        super::super::super::aggregate_digest(&[]).unwrap(),
    )
    .unwrap();
    let expected = Err::<(), _>("verifier's first failure");
    let (result, report) = run(&Connection::open_in_memory().unwrap(), &head, |_| expected);
    assert_eq!(result, expected);
    assert_eq!(report.outcome.status(), "unavailable");
    assert!(report.outcome.error().unwrap().contains("file-backed"));
    head.base_generation = Some(999);
    let (result, report) = run(&connection, &head, |_| expected);
    assert_eq!(result, expected);
    assert_eq!(report.outcome.status(), "unavailable");
    assert!(
        report
            .outcome
            .error()
            .unwrap()
            .contains("history hint identity")
    );
    checkpoint(&connection);
}

#[test]
fn history_read_ahead_null_predicate_does_not_materialize_the_json_value() {
    let (_dir, _path, connection) = fixture(0, 0);
    for value in ["NULL", "''", "'text'", "zeroblob(10)", "42"] {
        connection
            .execute_batch(&format!(
                "INSERT INTO activity_wallet_coverage_staging_v2 VALUES (1, 'wallet', '', {value})"
            ))
            .unwrap();
        let values: (bool, bool) = connection.query_row(
            "SELECT acquisition_json IS NOT NULL, CASE WHEN acquisition_json IS NOT NULL THEN 1 ELSE 0 END
             FROM activity_wallet_coverage_staging_v2", [], |row| Ok((row.get(0)?, row.get(1)?))
        ).unwrap();
        assert_eq!(values.0, values.1);
        connection
            .execute("DELETE FROM activity_wallet_coverage_staging_v2", [])
            .unwrap();
    }
    let mut statement = connection
        .prepare(&format!("EXPLAIN {ROWIDS_SQL}"))
        .unwrap();
    let column_flags = statement
        .query_map([1], |row| {
            Ok((row.get::<_, String>(1)?, row.get::<_, i64>(6)?))
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    let columns = column_flags
        .into_iter()
        .filter(|(opcode, _)| opcode == "Column")
        .collect::<Vec<_>>();
    assert_eq!(columns.len(), 1);
    assert_ne!(
        columns[0].1 & 0x80,
        0,
        "TYPEOFARG must suppress content loading"
    );
}
