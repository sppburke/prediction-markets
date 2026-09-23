//! Hashes the ranker projection in committed order using more than one core.
//!
//! The digest joins every projection row back to its activity group and payout
//! and hashes one sorted-key JSON object per row (#675). Through the caller's
//! own connection, which may hold a transaction, the join stays on the calling
//! thread; building and serializing each row's object moves to workers, and the
//! calling thread feeds the unchanged hasher in query order, so the committed
//! bytes do not change.
//!
//! A committed projection — at finalization, re-finalization and an outgoing
//! activation — is read from other connections instead. There the
//! join's random reads, one in flight at a time, are the bound (2,108 reads/s on
//! Forge's disk against 11,058 with eight in flight, measured 2026-09-23), so
//! readers on their own connections each take a key range and the caller hashes
//! the ranges in key order.

use std::mem;
use std::path::Path;
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::thread::{self, Scope};
use std::time::Duration;

use rusqlite::{Connection, OpenFlags, Rows, params};

use super::aggregate_scan::text_column;
use super::digests::JsonArrayDigest;
use super::{
    BootstrapError, RANKER_PROJECTION_BELOW_SQL, RANKER_PROJECTION_DIGEST_SQL,
    RANKER_PROJECTION_FROM_SQL, RANKER_PROJECTION_RANGE_SQL, to_i64,
};

/// The activity scan's batch, and its measured knee, carried over.
const BATCH_ROWS: usize = 512;
/// A four-core host runs this many workers beside its reader and consumer.
const MAX_WORKERS: usize = 3;
/// Connections reading a committed projection; they mostly wait on the disk.
const COMMITTED_READERS: usize = 8;
/// Column indexes carrying integers in `RANKER_PROJECTION_DIGEST_SQL`; the rest
/// are text. Both are read in column order, as the serial loop read them.
const INT_COLUMNS: [usize; 5] = [1, 2, 6, 11, 13];
const COLUMNS: usize = 14;
const TEXT_COLUMNS: usize = COLUMNS - INT_COLUMNS.len();

/// One batch of projection rows, packed into two buffers.
struct RawBatch {
    text: String,
    text_ends: Vec<usize>,
    ints: Vec<i64>,
    rows: usize,
}

impl RawBatch {
    fn new() -> Self {
        Self {
            text: String::new(),
            text_ends: Vec::new(),
            ints: Vec::new(),
            rows: 0,
        }
    }

    /// Reads one row's columns in their query order, so a null or mistyped
    /// column fails exactly where the serial loop's typed read failed. A failed
    /// row leaves the batch holding only whole rows.
    fn push(&mut self, row: &rusqlite::Row<'_>) -> Result<(), BootstrapError> {
        let committed = (self.text.len(), self.text_ends.len(), self.ints.len());
        match self.push_columns(row) {
            Ok(()) => {
                self.rows += 1;
                Ok(())
            }
            Err(error) => {
                self.text.truncate(committed.0);
                self.text_ends.truncate(committed.1);
                self.ints.truncate(committed.2);
                Err(error)
            }
        }
    }

    fn push_columns(&mut self, row: &rusqlite::Row<'_>) -> Result<(), BootstrapError> {
        for column in 0..COLUMNS {
            if INT_COLUMNS.contains(&column) {
                self.ints.push(row.get(column)?);
            } else {
                self.text.push_str(text_column(row, column)?);
                self.text_ends.push(self.text.len());
            }
        }
        Ok(())
    }

    fn full(&self) -> bool {
        self.rows >= BATCH_ROWS
    }
}

/// A batch's rows as canonical JSON, packed behind one buffer.
struct EncodedBatch {
    json: Vec<u8>,
    json_ends: Vec<usize>,
}

impl EncodedBatch {
    fn append(&mut self, batch: EncodedBatch) {
        let base = self.json.len();
        self.json.extend_from_slice(&batch.json);
        self.json_ends
            .extend(batch.json_ends.iter().map(|end| base + end));
    }
}

/// Builds the same object the serial loop built, key for key, and serializes it
/// with the same serializer, so the bytes are identical by construction.
fn encode_batch(batch: &RawBatch) -> Result<EncodedBatch, BootstrapError> {
    let mut json = Vec::new();
    let mut json_ends = Vec::with_capacity(batch.rows);
    let mut text_cursor = 0_usize;
    let mut text_index = 0_usize;
    for row in 0..batch.rows {
        let mut text: [&str; TEXT_COLUMNS] = [""; TEXT_COLUMNS];
        for field in &mut text {
            let end = batch.text_ends[text_index];
            *field = &batch.text[text_cursor..end];
            text_cursor = end;
            text_index += 1;
        }
        let ints = &batch.ints[row * INT_COLUMNS.len()..(row + 1) * INT_COLUMNS.len()];
        let value = serde_json::json!({
            "source_trade_id": text[0],
            "activity_generation": ints[0],
            "classifier_version": ints[1],
            "wallet_hex": text[1],
            "condition_id": text[2],
            "asset": text[3],
            "outcome_id": ints[2],
            "side": text[4],
            "share_amount_str": text[5],
            "price_weighted_share_amount_str": text[6],
            "source_usdc_amount_str": text[7],
            "source_time_unix": ints[3],
            "payout_vector_json": text[8],
            "end_date_unix": ints[4],
        });
        serde_json::to_writer(&mut json, &value)?;
        json_ends.push(json.len());
    }
    Ok(EncodedBatch { json, json_ends })
}

struct Worker<Job> {
    input: SyncSender<Job>,
    output: Receiver<Result<EncodedBatch, BootstrapError>>,
}

/// The digest of the generation's projection rows, in committed order.
pub(super) fn compute(
    connection: &Connection,
    activity_generation: u64,
) -> Result<String, BootstrapError> {
    let cores = thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);
    compute_with(
        MAX_WORKERS.min(cores.saturating_sub(1)).max(1),
        connection,
        activity_generation,
    )
}

/// The digest of a committed projection, with its join split across readers on
/// their own connections to `path`. The caller must hold the database's write
/// lock and must not have written in its transaction, so every reader sees
/// exactly the committed state the caller sees.
pub(super) fn compute_committed(
    path: &Path,
    activity_generation: u64,
) -> Result<String, BootstrapError> {
    compute_committed_with(COMMITTED_READERS, path, activity_generation)
}

/// Key ranges in key order that together hold every key a full read sees. The
/// ends are open; the table admits only `g2:` keys, which are hex digests, so
/// three hex characters split them evenly.
type KeyRange = (Option<String>, Option<String>);

fn key_ranges() -> Vec<KeyRange> {
    let mut bounds = vec![None];
    bounds.extend((1..4096).map(|prefix| Some(format!("g2:{prefix:03x}"))));
    bounds.push(None);
    bounds
        .windows(2)
        .map(|pair| (pair[0].clone(), pair[1].clone()))
        .collect()
}

fn compute_committed_with(
    readers: usize,
    path: &Path,
    activity_generation: u64,
) -> Result<String, BootstrapError> {
    let generation = to_i64(activity_generation, "activity generation")?;
    let path = std::fs::canonicalize(path)?;
    let ranges = key_ranges();
    thread::scope(|scope| {
        let workers = spawn_readers(scope, readers, &path, generation, &ranges)?;
        let mut cursor = Cursor::default();
        // Ranges are drained in dispatch order, which is key order, so the
        // first failing range in key order is the one reported.
        let outcome = (|| {
            let mut digest = JsonArrayDigest::new();
            let mut next = 0_usize;
            loop {
                while next < ranges.len() && cursor.in_flight < workers.len() * 2 {
                    dispatch(&workers, &mut cursor, next)?;
                    next += 1;
                }
                let Some(batch) = take(&workers, &mut cursor)? else {
                    return Ok(digest.finish());
                };
                hash(&mut digest, batch);
            }
        })();
        discard(&workers, &mut cursor);
        drop(workers);
        outcome
    })
}

/// Readers that each open their own read-only connection and encode whole key
/// ranges. A reader that cannot open or read fails certification; there is no
/// fallback to another read.
fn spawn_readers<'scope>(
    scope: &'scope Scope<'scope, '_>,
    readers: usize,
    path: &'scope Path,
    generation: i64,
    ranges: &'scope [KeyRange],
) -> Result<Vec<Worker<usize>>, BootstrapError> {
    (0..readers)
        .map(|index| {
            let (send_input, input) = sync_channel::<usize>(1);
            let (send_output, output) = sync_channel(2);
            thread::Builder::new()
                .name(format!("projection-read-{index}"))
                .spawn_scoped(scope, move || {
                    let mut connection = None;
                    while let Ok(range) = input.recv() {
                        let encoded = read_range(&mut connection, path, generation, &ranges[range]);
                        if send_output.send(encoded).is_err() {
                            break;
                        }
                    }
                })?;
            Ok(Worker {
                input: send_input,
                output,
            })
        })
        .collect()
}

fn read_range(
    connection: &mut Option<Connection>,
    path: &Path,
    generation: i64,
    range: &KeyRange,
) -> Result<EncodedBatch, BootstrapError> {
    if connection.is_none() {
        let opened = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        opened.busy_timeout(Duration::from_secs(5))?;
        *connection = Some(opened);
    }
    let connection = connection.as_ref().ok_or(BootstrapError::Internal)?;
    let (sql, bounds) = match range {
        (Some(lower), Some(upper)) => (RANKER_PROJECTION_RANGE_SQL, vec![lower, upper]),
        (None, Some(upper)) => (RANKER_PROJECTION_BELOW_SQL, vec![upper]),
        (Some(lower), None) => (RANKER_PROJECTION_FROM_SQL, vec![lower]),
        (None, None) => (RANKER_PROJECTION_DIGEST_SQL, Vec::new()),
    };
    let mut parameters: Vec<&dyn rusqlite::ToSql> = vec![&generation];
    parameters.extend(bounds.iter().map(|bound| *bound as &dyn rusqlite::ToSql));
    let mut statement = connection.prepare_cached(sql)?;
    let mut rows = statement.query(parameters.as_slice())?;
    let mut encoded = EncodedBatch {
        json: Vec::new(),
        json_ends: Vec::new(),
    };
    let mut batch = RawBatch::new();
    while let Some(row) = rows.next()? {
        batch.push(row)?;
        if batch.full() {
            encoded.append(encode_batch(&mem::replace(&mut batch, RawBatch::new()))?);
        }
    }
    if batch.rows > 0 {
        encoded.append(encode_batch(&batch)?);
    }
    Ok(encoded)
}

fn hash(digest: &mut JsonArrayDigest, batch: EncodedBatch) {
    let mut start = 0_usize;
    for end in batch.json_ends {
        digest.push_json(&batch.json[start..end]);
        start = end;
    }
}

fn compute_with(
    workers: usize,
    connection: &Connection,
    activity_generation: u64,
) -> Result<String, BootstrapError> {
    let generation = to_i64(activity_generation, "activity generation")?;
    thread::scope(|scope| {
        let workers = spawn(scope, workers)?;
        let mut statement = connection.prepare(RANKER_PROJECTION_DIGEST_SQL)?;
        let mut rows = statement.query(params![generation])?;
        let mut cursor = Cursor::default();
        let outcome = stream(&workers, &mut rows, &mut cursor);
        // Every outstanding batch is taken before the inputs close, so no
        // worker is left blocked when the scope joins them.
        discard(&workers, &mut cursor);
        drop(workers);
        outcome
    })
}

/// A pool that cannot be created fails certification rather than falling back
/// to a different read.
fn spawn<'scope>(
    scope: &'scope Scope<'scope, '_>,
    workers: usize,
) -> Result<Vec<Worker<RawBatch>>, BootstrapError> {
    (0..workers)
        .map(|index| {
            let (send_input, input) = sync_channel::<RawBatch>(1);
            // Two, so a worker never blocks on a full output: the caller admits
            // at most two batches per worker.
            let (send_output, output) = sync_channel(2);
            thread::Builder::new()
                .name(format!("projection-digest-{index}"))
                .spawn_scoped(scope, move || {
                    while let Ok(batch) = input.recv() {
                        if send_output.send(encode_batch(&batch)).is_err() {
                            break;
                        }
                    }
                })?;
            Ok(Worker {
                input: send_input,
                output,
            })
        })
        .collect()
}

fn stream(
    workers: &[Worker<RawBatch>],
    rows: &mut Rows<'_>,
    cursor: &mut Cursor,
) -> Result<String, BootstrapError> {
    let mut digest = JsonArrayDigest::new();
    let mut current = RawBatch::new();
    let mut exhausted = false;
    let mut read_failure = None;
    loop {
        while !exhausted && read_failure.is_none() && cursor.in_flight < workers.len() * 2 {
            match rows.next() {
                Ok(None) => exhausted = true,
                Ok(Some(row)) => match current.push(row) {
                    // Rows read before a failing one are still hashed first,
                    // as the serial loop hashed them before returning.
                    Err(error) => read_failure = Some(error),
                    Ok(()) => {
                        if current.full() {
                            dispatch(workers, cursor, mem::replace(&mut current, RawBatch::new()))?;
                        }
                    }
                },
                Err(error) => read_failure = Some(BootstrapError::from(error)),
            }
        }
        if (exhausted || read_failure.is_some())
            && current.rows > 0
            && cursor.in_flight < workers.len() * 2
        {
            dispatch(workers, cursor, mem::replace(&mut current, RawBatch::new()))?;
        }
        let Some(batch) = take(workers, cursor)? else {
            break;
        };
        hash(&mut digest, batch);
    }
    match read_failure {
        Some(error) => Err(error),
        None => Ok(digest.finish()),
    }
}

fn dispatch<Job>(
    workers: &[Worker<Job>],
    cursor: &mut Cursor,
    job: Job,
) -> Result<(), BootstrapError> {
    workers[cursor.dispatch % workers.len()]
        .input
        .send(job)
        .map_err(|_| BootstrapError::Internal)?;
    cursor.dispatch += 1;
    cursor.in_flight += 1;
    Ok(())
}

/// Takes the next batch in dispatch order, or `None` once none is in flight.
fn take<Job>(
    workers: &[Worker<Job>],
    cursor: &mut Cursor,
) -> Result<Option<EncodedBatch>, BootstrapError> {
    if cursor.in_flight == 0 {
        return Ok(None);
    }
    let batch = workers[cursor.drain % workers.len()]
        .output
        .recv()
        .map_err(|_| BootstrapError::Internal)?;
    cursor.drain += 1;
    cursor.in_flight -= 1;
    batch.map(Some)
}

fn discard<Job>(workers: &[Worker<Job>], cursor: &mut Cursor) {
    while cursor.in_flight > 0 {
        // A worker that has already stopped returns an error at once; every
        // other worker's batch must still be taken.
        let _ = workers[cursor.drain % workers.len()].output.recv();
        cursor.drain += 1;
        cursor.in_flight -= 1;
    }
}

#[derive(Default)]
struct Cursor {
    dispatch: usize,
    drain: usize,
    in_flight: usize,
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "a broken fixture must fail its test")]
mod tests {
    use rusqlite::Connection;
    use tempfile::TempDir;

    use super::super::V2_SCHEMA;
    use super::super::activity_fixtures::{GENERATION, built_aggregates, seed, wallet_hex};
    use super::*;

    const WORKER_COUNTS: [usize; 2] = [1, 3];
    const READER_COUNTS: [usize; 3] = [1, 3, 8];

    /// The serial loop this module replaced, kept as the reference.
    fn serial_digest(connection: &Connection) -> Result<String, BootstrapError> {
        let mut statement = connection.prepare(RANKER_PROJECTION_DIGEST_SQL)?;
        let rows = statement.query_map(params![GENERATION], |row| {
            Ok(serde_json::json!({
                "source_trade_id": row.get::<_, String>(0)?,
                "activity_generation": row.get::<_, i64>(1)?,
                "classifier_version": row.get::<_, i64>(2)?,
                "wallet_hex": row.get::<_, String>(3)?,
                "condition_id": row.get::<_, String>(4)?,
                "asset": row.get::<_, String>(5)?,
                "outcome_id": row.get::<_, i64>(6)?,
                "side": row.get::<_, String>(7)?,
                "share_amount_str": row.get::<_, String>(8)?,
                "price_weighted_share_amount_str": row.get::<_, String>(9)?,
                "source_usdc_amount_str": row.get::<_, String>(10)?,
                "source_time_unix": row.get::<_, i64>(11)?,
                "payout_vector_json": row.get::<_, String>(12)?,
                "end_date_unix": row.get::<_, i64>(13)?,
            }))
        })?;
        let mut digest = JsonArrayDigest::new();
        for row in rows {
            digest.push(&row?)?;
        }
        Ok(digest.finish())
    }

    /// A projection over `projected` of `groups` activity rows, each joined to a
    /// resolved payout, in a cache whose walk committed those payouts.
    fn projected_cache(groups: usize, projected: usize) -> (TempDir, Connection) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("cache.db");
        // The payout tables and their migrations come from the cache's own
        // opener; the lane-A tables come from the schema-two owner.
        drop(crate::cache::WalletCache::open(&path).unwrap());
        let mut connection = Connection::open(&path).unwrap();
        connection.execute_batch(V2_SCHEMA).unwrap();
        let wallet = wallet_hex(1);
        let aggregates = built_aggregates(&wallet, groups);
        seed(&mut connection, &wallet, &aggregates);
        let transaction = connection.transaction().unwrap();
        transaction
            .execute(
                "INSERT INTO clob_payout_coverage_manifests_v2
                     (generation, manifest_json, walked_start_cursor, walked_end_cursor,
                      page_count, market_count, closed_market_count, resolved_payout_count,
                      unresolved_payout_count, explicit_fifty_fifty_count, terminal_kind,
                      terminal_page_sha256, schema_version, parser_version, completed_at_unix,
                      evidence_count)
                 VALUES (1, '{}', NULL, 'LTE=', 1, ?1, ?1, ?1, 0, 0, 'end_cursor', ?2, 2, 2,
                         1800000020, ?1)",
                params![i64::try_from(groups).unwrap(), "a".repeat(64)],
            )
            .unwrap();
        for (index, aggregate) in aggregates.iter().enumerate() {
            let market = aggregate
                .group_id
                .components()
                .condition_id
                .as_ref()
                .unwrap()
                .to_string();
            transaction
                .execute(
                    "INSERT INTO clob_payout_evidence_v2
                         (market_id, end_date_unix, is_50_50_outcome, payout_status,
                          payout_vector_json, closed, tokens_json, raw_page_sha256,
                          coverage_generation, page_ordinal, schema_version, parser_version,
                          fetched_at_unix, origin)
                     VALUES (?1, ?2, 0, 'resolved', ?3, 1, '[]', ?4, 1, 0, 2, 2, 1800000010,
                             'clob_closed_walk_v2')",
                    params![
                        market,
                        1_800_000_000 + i64::try_from(index).unwrap(),
                        if index % 2 == 0 {
                            "[\"1\",\"0\"]"
                        } else {
                            "[\"0\",\"1\"]"
                        },
                        "b".repeat(64),
                    ],
                )
                .unwrap();
            if index < projected {
                transaction
                    .execute(
                        "INSERT INTO ranker_entries_v2
                             (source_trade_id, activity_generation, classifier_version)
                         VALUES (?1, ?2, 2)",
                        params![aggregate.group_id.key().0, GENERATION],
                    )
                    .unwrap();
            }
        }
        transaction.commit().unwrap();
        (dir, connection)
    }

    #[test]
    fn the_parallel_digest_equals_the_serial_one_across_batch_boundaries() {
        // Every size straddles or fills a batch differently; zero covers an
        // empty projection, whose digest is the empty array's.
        // 5,000 rows is ten batches, past the six a three-worker pool may hold.
        for projected in [
            0,
            1,
            BATCH_ROWS - 1,
            BATCH_ROWS,
            BATCH_ROWS + 1,
            2_000,
            5_000,
        ] {
            let (dir, connection) = projected_cache(projected.max(1), projected);
            if projected > 0 {
                // Bytes the serializer must escape, and integers at their extremes.
                connection
                    .execute_batch(
                        "UPDATE activity_groups_v2 SET share_amount_str = 'a\"b\\c\u{1}d' \
                             WHERE rowid % 7 = 0;
                         UPDATE activity_groups_v2 SET asset = 'x\u{e9}\u{1f4b0}\u{2028}y' \
                             WHERE rowid % 11 = 0;
                         UPDATE activity_groups_v2 SET source_time_unix = 9223372036854775807 \
                             WHERE rowid % 13 = 0;
                         UPDATE activity_groups_v2 SET outcome_id = -9223372036854775808 \
                             WHERE rowid % 17 = 0;",
                    )
                    .unwrap();
            }
            let expected = serial_digest(&connection).unwrap();
            for workers in WORKER_COUNTS {
                let actual = compute_with(workers, &connection, 1).unwrap();
                assert_eq!(actual, expected, "{projected} rows, {workers} worker(s)");
            }
            for readers in READER_COUNTS {
                let actual =
                    compute_committed_with(readers, &dir.path().join("cache.db"), 1).unwrap();
                assert_eq!(actual, expected, "{projected} rows, {readers} reader(s)");
            }
        }
    }

    #[test]
    fn key_ranges_hold_every_key_once_in_order() {
        let ranges = key_ranges();
        assert_eq!(ranges.len(), 4096);
        assert_eq!(ranges[0], (None, Some("g2:001".to_owned())));
        assert_eq!(ranges[4095], (Some("g2:fff".to_owned()), None));
        assert!(ranges.windows(2).all(|pair| pair[0].1 == pair[1].0));
        let range_of = |key: &str| {
            let hits: Vec<_> = ranges
                .iter()
                .enumerate()
                .filter(|(_, (lower, upper))| {
                    lower.as_deref().is_none_or(|lower| lower <= key)
                        && upper.as_deref().is_none_or(|upper| key < upper)
                })
                .map(|(index, _)| index)
                .collect();
            assert_eq!(hits.len(), 1, "{key}");
            hits[0]
        };
        assert_eq!(range_of(""), 0);
        assert_eq!(range_of(&format!("g2:{}", "0".repeat(64))), 0);
        assert_eq!(range_of("g2:001"), 1);
        assert_eq!(range_of(&format!("g2:abc{}", "9".repeat(61))), 0xabc);
        assert_eq!(range_of(&format!("g2:{}", "f".repeat(64))), 4095);
        assert_eq!(range_of("g3:extra"), 4095);
    }

    /// Rekeys the first `rekeyed` projection rows, and their activity rows,
    /// past the table's check: keys below and above every `g2:` key, exact
    /// range bounds, and the rest crowded into one range.
    fn rekey(connection: &Connection, rekeyed: usize) {
        connection
            .execute_batch(&format!(
                "PRAGMA ignore_check_constraints = ON;
                 CREATE TEMP TABLE rekey AS
                 SELECT old, CASE n WHEN 0 THEN 'a' WHEN 1 THEN 'g2:001' WHEN 2 THEN 'g2:fff'
                                    WHEN 3 THEN 'g3:extra' WHEN 4 THEN 'zzz'
                                    ELSE 'g2:abc' || substr(old, 7) END AS new
                 FROM (SELECT source_trade_id AS old,
                              row_number() OVER (ORDER BY source_trade_id) - 1 AS n
                       FROM ranker_entries_v2)
                 WHERE n < {rekeyed};
                 UPDATE activity_groups_v2 SET source_trade_id =
                     (SELECT new FROM rekey WHERE old = source_trade_id)
                 WHERE source_trade_id IN (SELECT old FROM rekey);
                 UPDATE ranker_entries_v2 SET source_trade_id =
                     (SELECT new FROM rekey WHERE old = source_trade_id)
                 WHERE source_trade_id IN (SELECT old FROM rekey);
                 DROP TABLE rekey;
                 PRAGMA ignore_check_constraints = OFF;"
            ))
            .unwrap();
    }

    #[test]
    fn keys_outside_the_prefix_bounds_and_crowded_ranges_hash_as_a_full_read() {
        // A key that bypassed the table's check is still read, and hashed in
        // its place, as the full read hashes it; one range holding far more
        // than a batch still hashes every row in order.
        let (dir, connection) = projected_cache(1_300, 1_300);
        rekey(&connection, 1_100);
        let crowded: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM ranker_entries_v2 WHERE source_trade_id LIKE 'g2:abc%'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(
            crowded > i64::try_from(BATCH_ROWS).unwrap() + 1,
            "{crowded}"
        );
        let expected = serial_digest(&connection).unwrap();
        for readers in READER_COUNTS {
            assert_eq!(
                compute_committed_with(readers, &dir.path().join("cache.db"), 1).unwrap(),
                expected,
                "{readers} reader(s)"
            );
        }
    }

    #[test]
    fn readers_see_the_committed_state_the_write_lock_holder_sees() {
        // A commit still in the log, not the main file, is what the readers
        // hash; while the lock is held no other writer can change it.
        let (dir, mut connection) = projected_cache(900, 900);
        connection
            .execute_batch(
                "PRAGMA journal_mode = WAL;
                 PRAGMA wal_autocheckpoint = 0;
                 UPDATE activity_groups_v2 SET asset = 'changed' WHERE rowid % 3 = 0;",
            )
            .unwrap();
        let transaction = connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .unwrap();
        let expected = serial_digest(&transaction).unwrap();
        let path = dir.path().join("cache.db");
        let other = Connection::open(&path).unwrap();
        other.busy_timeout(Duration::ZERO).unwrap();
        assert!(
            other
                .execute("UPDATE activity_groups_v2 SET asset = 'late'", [])
                .is_err()
        );
        assert_eq!(compute_committed_with(8, &path, 1).unwrap(), expected);
        transaction.rollback().unwrap();
    }

    #[test]
    fn the_first_failing_range_in_key_order_is_reported() {
        // Two failing rows in the adjacent ranges `g2:001` and `g2:002`, both
        // admitted at once: the earlier range's error is the one reported,
        // as the serial read reports it, whichever reader finishes first.
        let (dir, connection) = projected_cache(2_000, 2_000);
        let rows = [
            (
                "g2:001",
                "UPDATE activity_groups_v2 SET side = NULL WHERE source_trade_id = ?1",
            ),
            (
                "g2:002",
                "UPDATE activity_groups_v2 SET asset = NULL WHERE source_trade_id = ?1",
            ),
        ];
        for (offset, (prefix, damage)) in rows.into_iter().enumerate() {
            let old: String = connection
                .query_row(
                    "SELECT source_trade_id FROM ranker_entries_v2 ORDER BY source_trade_id
                     LIMIT 1 OFFSET ?1",
                    [300 + i64::try_from(offset).unwrap() * 1_000],
                    |row| row.get(0),
                )
                .unwrap();
            let new = format!("{prefix}{}", &old[6..]);
            for table in ["activity_groups_v2", "ranker_entries_v2"] {
                connection
                    .execute(
                        &format!(
                            "UPDATE {table} SET source_trade_id = ?2 WHERE source_trade_id = ?1"
                        ),
                        params![old, new],
                    )
                    .unwrap();
            }
            connection.execute(damage, params![new]).unwrap();
        }
        let expected = serial_digest(&connection).unwrap_err().to_string();
        assert!(expected.contains("side"), "{expected}");
        for readers in READER_COUNTS {
            let actual = compute_committed_with(readers, &dir.path().join("cache.db"), 1)
                .unwrap_err()
                .to_string();
            assert_eq!(actual, expected, "{readers} reader(s)");
        }
    }

    #[test]
    fn a_reader_that_cannot_open_or_read_the_cache_fails_without_fallback() {
        // A directory cannot be opened as a database at all; a file that is
        // not one opens and then fails its first read.
        let dir = TempDir::new().unwrap();
        let garbage = dir.path().join("not-a-cache.db");
        std::fs::write(&garbage, vec![b'x'; 8192]).unwrap();
        for path in [dir.path().to_path_buf(), garbage] {
            for readers in READER_COUNTS {
                assert!(
                    compute_committed_with(readers, &path, 1).is_err(),
                    "{} with {readers} reader(s)",
                    path.display()
                );
            }
        }
    }

    #[test]
    fn a_column_the_typed_read_rejects_fails_with_the_same_error() {
        let (dir, connection) = projected_cache(700, 700);
        // A null where the digest reads text: the serial loop's typed read
        // fails at that row, and so must this one, with the same text.
        let victim: String = connection
            .query_row(
                "SELECT source_trade_id FROM ranker_entries_v2 ORDER BY source_trade_id
                 LIMIT 1 OFFSET 600",
                [],
                |row| row.get(0),
            )
            .unwrap();
        connection
            .execute(
                "UPDATE activity_groups_v2 SET side = NULL WHERE source_trade_id = ?1",
                params![victim],
            )
            .unwrap();
        let expected = serial_digest(&connection).unwrap_err().to_string();
        for workers in WORKER_COUNTS {
            let actual = compute_with(workers, &connection, 1)
                .unwrap_err()
                .to_string();
            assert_eq!(actual, expected, "{workers} worker(s)");
        }
        for readers in READER_COUNTS {
            let actual = compute_committed_with(readers, &dir.path().join("cache.db"), 1)
                .unwrap_err()
                .to_string();
            assert_eq!(actual, expected, "{readers} reader(s)");
        }
    }

    #[test]
    fn rows_are_hashed_in_committed_order_not_arrival_order() {
        // Reversing the projection's insertion order must not move a single
        // byte: the query's ORDER BY, not the table, fixes the sequence.
        let (dir, connection) = projected_cache(1_300, 1_300);
        let expected = serial_digest(&connection).unwrap();
        connection
            .execute_batch(
                "CREATE TEMP TABLE reversed AS SELECT * FROM ranker_entries_v2
                     ORDER BY source_trade_id DESC;
                 DELETE FROM ranker_entries_v2;
                 INSERT INTO ranker_entries_v2 SELECT * FROM reversed;",
            )
            .unwrap();
        assert_eq!(compute_with(3, &connection, 1).unwrap(), expected);
        assert_eq!(
            compute_committed_with(8, &dir.path().join("cache.db"), 1).unwrap(),
            expected
        );
    }
}
