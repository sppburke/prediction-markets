//! Reads a wallet's stored activity aggregates using more than one core.
//!
//! Decoding a stored row and serializing its canonical JSON are the validation
//! traversal's dominant cost, and both are per row (#670). One reader packs raw
//! column bytes into contiguous batches, workers decode and serialize them, and
//! the caller consumes batches in stored order, so every commitment still sees
//! the same bytes in the same sequence a serial read produces.
//!
//! The serial read this replaced ran in phases: it decoded a whole wallet,
//! counted it, then serialized it. Streaming detects failures earlier than that,
//! so a failure is carried to the phase that used to report it rather than
//! returned where it was found.

use std::collections::VecDeque;
use std::mem;
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::thread::{self, Scope};

use rusqlite::types::ValueRef;
use rusqlite::{Connection, Row, Rows, params};

use super::{ActivityAggregate, BootstrapError, StoredActivityRow, decode_activity_aggregate};

/// The carry loop's keyset window and this pipeline's measured knee agree.
const BATCH_ROWS: usize = 512;
/// A four-core host runs this many workers beside its reader and consumer.
const MAX_WORKERS: usize = 3;
/// Stored rows carry unbounded text, so a batch also ends once its packed
/// columns reach this size.
const BATCH_TEXT_BYTES: usize = 4 * 1024 * 1024;
/// Packed columns queued across all workers. One batch is always admitted, so
/// rows wider than this still make progress, alone.
const OUTSTANDING_TEXT_BYTES: usize = 32 * 1024 * 1024;
/// Column indexes carrying stored integers; the rest are text.
const INT_COLUMNS: [usize; 3] = [3, 7, 8];
const COLUMNS: usize = 9;
const TEXT_COLUMNS: usize = COLUMNS - INT_COLUMNS.len();

const SELECT_AGGREGATES: &str =
    "SELECT source_trade_id, semantic_revision, components_json, row_count,
                share_amount_str, price_weighted_share_amount_str, source_usdc_amount_str,
                source_time_unix, is_combo
         FROM activity_groups_v2
         WHERE coverage_generation = ?1 AND wallet_hex = ?2
         ORDER BY source_time_unix, source_trade_id";

/// One batch of stored rows, packed into two buffers instead of six owned
/// strings and three integer reads per row. Workers rebuild the owned values
/// the decoder expects.
struct RawBatch {
    wallet_hex: String,
    text: String,
    text_ends: Vec<u32>,
    ints: Vec<i64>,
    rows: usize,
}

impl RawBatch {
    fn new(wallet_hex: &str) -> Self {
        Self {
            wallet_hex: wallet_hex.to_owned(),
            text: String::new(),
            text_ends: Vec::new(),
            ints: Vec::new(),
            rows: 0,
        }
    }

    /// Reads one row's columns in their stored order, so a malformed column
    /// fails exactly where a typed read of the same row fails. A failed row
    /// leaves the batch holding only whole rows, which stay dispatchable.
    fn push(&mut self, row: &Row<'_>) -> Result<(), BootstrapError> {
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

    fn push_columns(&mut self, row: &Row<'_>) -> Result<(), BootstrapError> {
        for column in 0..COLUMNS {
            if INT_COLUMNS.contains(&column) {
                self.ints.push(row.get(column)?);
            } else {
                self.text.push_str(text_column(row, column)?);
                self.text_ends
                    .push(u32::try_from(self.text.len()).map_err(|_| BootstrapError::Internal)?);
            }
        }
        Ok(())
    }

    fn full(&self) -> bool {
        self.rows >= BATCH_ROWS || self.text.len() >= BATCH_TEXT_BYTES
    }
}

/// A batch's aggregates with their canonical JSON packed behind one buffer.
///
/// `json_ends` covers the rows serialized before `serialization_failure`, which
/// the phased read reported only after counting a whole wallet.
struct DecodedBatch {
    aggregates: Vec<ActivityAggregate>,
    json: Vec<u8>,
    json_ends: Vec<u32>,
    serialization_failure: Option<BootstrapError>,
}

pub(super) fn text_column<'row>(
    row: &'row Row<'_>,
    column: usize,
) -> Result<&'row str, BootstrapError> {
    if let ValueRef::Text(bytes) = row.get_ref(column)?
        && let Ok(text) = std::str::from_utf8(bytes)
    {
        return Ok(text);
    }
    // A stored value this read cannot use reports the typed read's own error,
    // which names the column and its type; that read cannot succeed here.
    row.get::<_, String>(column)?;
    Err(BootstrapError::Internal)
}

fn decode_batch(batch: &RawBatch) -> Result<DecodedBatch, BootstrapError> {
    // Decoding the whole batch before serializing any of it keeps a decode
    // failure ahead of a serialization failure, as the phased read had it.
    let mut aggregates = Vec::with_capacity(batch.rows);
    let mut text_cursor = 0_usize;
    let mut text_index = 0_usize;
    for row in 0..batch.rows {
        let mut fields: [&str; TEXT_COLUMNS] = [""; TEXT_COLUMNS];
        for field in &mut fields {
            let end = batch.text_ends[text_index] as usize;
            *field = &batch.text[text_cursor..end];
            text_cursor = end;
            text_index += 1;
        }
        let ints = &batch.ints[row * INT_COLUMNS.len()..];
        let stored: StoredActivityRow = (
            fields[0].to_owned(),
            fields[1].to_owned(),
            fields[2].to_owned(),
            ints[0],
            fields[3].to_owned(),
            fields[4].to_owned(),
            fields[5].to_owned(),
            ints[1],
            ints[2],
        );
        aggregates.push(decode_activity_aggregate(stored, &batch.wallet_hex)?);
    }
    let mut json = Vec::new();
    let mut json_ends = Vec::with_capacity(batch.rows);
    let mut serialization_failure = None;
    for aggregate in &aggregates {
        let committed = json.len();
        if let Err(error) = serde_json::to_writer(&mut json, aggregate) {
            json.truncate(committed);
            serialization_failure = Some(BootstrapError::from(error));
            break;
        }
        match u32::try_from(json.len()) {
            Ok(end) => json_ends.push(end),
            Err(_) => {
                json.truncate(committed);
                serialization_failure = Some(BootstrapError::Internal);
                break;
            }
        }
    }
    Ok(DecodedBatch {
        aggregates,
        json,
        json_ends,
        serialization_failure,
    })
}

struct Worker {
    input: SyncSender<RawBatch>,
    output: Receiver<Result<DecodedBatch, BootstrapError>>,
}

/// A wallet-ordered reader over one worker pool, reused across a traversal.
pub(super) struct Scan {
    workers: Vec<Worker>,
}

/// Runs `work` with a pool sized for this host; the pool stops when it returns.
pub(super) fn scoped<T>(
    work: impl FnOnce(&mut Scan) -> Result<T, BootstrapError>,
) -> Result<T, BootstrapError> {
    let cores = thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);
    scoped_with(MAX_WORKERS.min(cores.saturating_sub(1)).max(1), work)
}

fn scoped_with<T>(
    workers: usize,
    work: impl FnOnce(&mut Scan) -> Result<T, BootstrapError>,
) -> Result<T, BootstrapError> {
    thread::scope(|scope| {
        let mut scan = Scan::new(scope, workers)?;
        let outcome = work(&mut scan);
        // Dropping the inputs ends every worker before the scope joins them.
        drop(scan);
        outcome
    })
}

impl Scan {
    /// A pool that cannot be created fails certification rather than falling
    /// back to a different read.
    fn new<'scope>(
        scope: &'scope Scope<'scope, '_>,
        workers: usize,
    ) -> Result<Self, BootstrapError> {
        let workers = (0..workers)
            .map(|index| {
                let (send_input, input) = sync_channel::<RawBatch>(1);
                // Two, so a worker never blocks on a full output: the caller
                // admits at most two batches per worker.
                let (send_output, output) = sync_channel(2);
                thread::Builder::new()
                    .name(format!("activity-scan-{index}"))
                    .spawn_scoped(scope, move || {
                        while let Ok(batch) = input.recv() {
                            if send_output.send(decode_batch(&batch)).is_err() {
                                break;
                            }
                        }
                    })?;
                Ok(Worker {
                    input: send_input,
                    output,
                })
            })
            .collect::<Result<Vec<_>, BootstrapError>>()?;
        Ok(Self { workers })
    }

    /// Calls `consume` with every stored aggregate of one wallet, in stored
    /// order, with the canonical JSON its commitments are built from.
    ///
    /// `Ok(Some(error))` means every row was delivered but one could not be
    /// serialized: that row and the rest of its batch carry no JSON, and the
    /// caller reports the failure where the phased read reported it — after
    /// counting the wallet, before comparing its receipt.
    pub(super) fn for_each(
        &mut self,
        connection: &Connection,
        generation: i64,
        wallet_hex: &str,
        consume: impl FnMut(ActivityAggregate, Option<&[u8]>) -> Result<(), BootstrapError>,
    ) -> Result<Option<BootstrapError>, BootstrapError> {
        let mut statement = connection.prepare(SELECT_AGGREGATES)?;
        let mut rows = statement.query(params![generation, wallet_hex])?;
        let mut cursor = Cursor::default();
        let outcome = self.stream(&mut rows, wallet_hex, &mut cursor, consume);
        // Whatever ended the wallet, the pool must hold nothing when the next
        // wallet starts, or its batches would be consumed out of order.
        self.discard(&mut cursor);
        outcome
    }

    fn stream(
        &mut self,
        rows: &mut Rows<'_>,
        wallet_hex: &str,
        cursor: &mut Cursor,
        mut consume: impl FnMut(ActivityAggregate, Option<&[u8]>) -> Result<(), BootstrapError>,
    ) -> Result<Option<BootstrapError>, BootstrapError> {
        let mut current = RawBatch::new(wallet_hex);
        let mut exhausted = false;
        let mut read_failure = None;
        let mut serialization_failure: Option<BootstrapError> = None;
        loop {
            while !exhausted && read_failure.is_none() && self.has_room(cursor) {
                match rows.next() {
                    Ok(None) => exhausted = true,
                    Ok(Some(row)) => match current.push(row) {
                        // Rows read before a failing one still decode, so an
                        // earlier row's failure still precedes this one.
                        Err(error) => read_failure = Some(error),
                        Ok(()) => {
                            if current.full() {
                                self.dispatch(cursor, &mut current, wallet_hex)?;
                            }
                        }
                    },
                    Err(error) => read_failure = Some(BootstrapError::from(error)),
                }
            }
            if (exhausted || read_failure.is_some()) && current.rows > 0 && self.has_room(cursor) {
                self.dispatch(cursor, &mut current, wallet_hex)?;
            }
            let Some(batch) = self.take(cursor)? else {
                break;
            };
            let DecodedBatch {
                aggregates,
                json,
                json_ends,
                serialization_failure: failed,
            } = batch;
            let mut start = 0_usize;
            for (index, aggregate) in aggregates.into_iter().enumerate() {
                match json_ends.get(index) {
                    Some(end) => {
                        let end = *end as usize;
                        consume(aggregate, Some(&json[start..end]))?;
                        start = end;
                    }
                    // Every row is still delivered, so the wallet's counts are
                    // complete before its serialization failure is reported.
                    None => consume(aggregate, None)?,
                }
            }
            if serialization_failure.is_none() {
                serialization_failure = failed;
            }
        }
        match read_failure {
            Some(error) => Err(error),
            None => Ok(serialization_failure),
        }
    }

    /// A worker may hold one queued batch beside the one it is decoding, and
    /// the packed columns in flight are bounded; one batch is always admitted.
    fn has_room(&self, cursor: &Cursor) -> bool {
        cursor.in_flight < self.workers.len() * 2
            && (cursor.in_flight == 0 || cursor.outstanding_bytes < OUTSTANDING_TEXT_BYTES)
    }

    fn dispatch(
        &self,
        cursor: &mut Cursor,
        current: &mut RawBatch,
        wallet_hex: &str,
    ) -> Result<(), BootstrapError> {
        let batch = mem::replace(current, RawBatch::new(wallet_hex));
        let bytes = batch.text.len();
        let worker = &self.workers[cursor.dispatch % self.workers.len()];
        worker
            .input
            .send(batch)
            .map_err(|_| BootstrapError::Internal)?;
        cursor.dispatch += 1;
        cursor.in_flight += 1;
        cursor.outstanding_bytes += bytes;
        cursor.sizes.push_back(bytes);
        Ok(())
    }

    /// Takes the next batch in stored order, or `None` once none is in flight.
    fn take(&mut self, cursor: &mut Cursor) -> Result<Option<DecodedBatch>, BootstrapError> {
        if cursor.in_flight == 0 {
            return Ok(None);
        }
        let worker = &self.workers[cursor.drain % self.workers.len()];
        let batch = worker.output.recv().map_err(|_| BootstrapError::Internal)?;
        cursor.settle();
        batch.map(Some)
    }

    fn discard(&mut self, cursor: &mut Cursor) {
        while cursor.in_flight > 0 {
            let worker = &self.workers[cursor.drain % self.workers.len()];
            // A worker that has already stopped returns an error at once; every
            // other worker's batch must still be taken.
            let _ = worker.output.recv();
            cursor.settle();
        }
    }
}

#[derive(Default)]
struct Cursor {
    dispatch: usize,
    drain: usize,
    in_flight: usize,
    outstanding_bytes: usize,
    sizes: VecDeque<usize>,
}

impl Cursor {
    fn settle(&mut self) {
        self.drain += 1;
        self.in_flight -= 1;
        self.outstanding_bytes -= self.sizes.pop_front().unwrap_or(0);
    }
}

#[cfg(test)]
#[expect(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "a broken fixture must fail its test"
)]
mod tests {
    use rusqlite::Connection;

    use super::super::activity_fixtures::{
        GENERATION, same_second_aggregates, seed, seeded, wallet_hex,
    };
    use super::super::{V2_SCHEMA, canonical_json, invalid};
    use super::*;

    /// Both the one-worker arrangement a two-core host produces and the
    /// three-worker arrangement Forge produces must read identically.
    const WORKER_COUNTS: [usize; 2] = [1, 3];

    /// The serial read this scan replaced: one statement, rows decoded and
    /// serialized in stored order.
    fn serial_read(
        connection: &Connection,
        wallet_hex: &str,
    ) -> Result<Vec<(ActivityAggregate, String)>, BootstrapError> {
        let mut statement = connection.prepare(SELECT_AGGREGATES)?;
        let rows = statement.query_map(params![GENERATION, wallet_hex], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, i64>(7)?,
                row.get::<_, i64>(8)?,
            ))
        })?;
        let aggregates = rows
            .map(|row| decode_activity_aggregate(row?, wallet_hex))
            .collect::<Result<Vec<_>, _>>()?;
        aggregates
            .into_iter()
            .map(|aggregate| {
                let json = canonical_json(&aggregate)?;
                Ok((aggregate, json))
            })
            .collect()
    }

    fn read_wallet(
        scan: &mut Scan,
        connection: &Connection,
        wallet: &str,
    ) -> Result<Vec<(ActivityAggregate, Option<String>)>, BootstrapError> {
        let mut read = Vec::new();
        assert!(
            scan.for_each(connection, GENERATION, wallet, |aggregate, json| {
                read.push((
                    aggregate,
                    json.map(|bytes| String::from_utf8_lossy(bytes).into_owned()),
                ));
                Ok(())
            })?
            .is_none(),
            "fixture wallets serialize"
        );
        Ok(read)
    }

    fn assert_matches_serial(
        connection: &Connection,
        wallet: &str,
        read: &[(ActivityAggregate, Option<String>)],
    ) {
        let expected = serial_read(connection, wallet).unwrap();
        assert_eq!(read.len(), expected.len(), "wallet {wallet} row count");
        for (index, (scanned, expected)) in read.iter().zip(&expected).enumerate() {
            assert_eq!(scanned.0, expected.0, "wallet {wallet} aggregate {index}");
            assert_eq!(
                scanned.1.as_deref(),
                Some(expected.1.as_str()),
                "wallet {wallet} canonical JSON {index}"
            );
        }
    }

    #[test]
    fn scanned_wallets_match_a_serial_read_exactly() {
        // Sizes straddle the batch boundary in both directions and include an
        // empty wallet, a single row, and more than ten batches.
        let sizes = [0, 1, BATCH_ROWS - 1, BATCH_ROWS, BATCH_ROWS + 1, 5_200];
        let (connection, wallets) = seeded(&sizes);
        for workers in WORKER_COUNTS {
            let read = scoped_with(workers, |scan| {
                wallets
                    .iter()
                    .map(|wallet| read_wallet(scan, &connection, wallet))
                    .collect::<Result<Vec<_>, _>>()
            })
            .unwrap();
            for ((wallet, size), read) in wallets.iter().zip(sizes).zip(&read) {
                assert_eq!(read.len(), size, "wallet {wallet} fixture size");
                assert_matches_serial(&connection, wallet, read);
            }
        }
    }

    #[test]
    fn equal_seconds_keep_their_identity_order_across_batches() {
        let wallet = wallet_hex(9);
        let mut connection = Connection::open_in_memory().unwrap();
        connection.execute_batch(V2_SCHEMA).unwrap();
        seed(
            &mut connection,
            &wallet,
            &same_second_aggregates(&wallet, BATCH_ROWS * 3 + 7),
        );
        for workers in WORKER_COUNTS {
            let read =
                scoped_with(workers, |scan| read_wallet(scan, &connection, &wallet)).unwrap();
            assert_matches_serial(&connection, &wallet, &read);
            let ordered = read
                .windows(2)
                .all(|pair| pair[0].0.group_id.key().0 < pair[1].0.group_id.key().0);
            assert!(ordered, "equal seconds must stay in identity order");
        }
    }

    #[test]
    fn the_first_unreadable_row_in_stored_order_fails_the_wallet() {
        let (connection, wallets) = seeded(&[1_600, 600]);
        let expected = serial_read(&connection, &wallets[0]).unwrap();
        // Two rows in different batches lose their identity; the earlier one
        // must be the failure, exactly as a serial read reports it.
        for index in [700, 1_300] {
            connection
                .execute(
                    "UPDATE activity_groups_v2
                     SET components_json = json_set(components_json, '$.transaction_hash', 'changed')
                     WHERE source_trade_id = ?1",
                    params![expected[index].0.group_id.key().0],
                )
                .unwrap();
        }
        let expected_message = format!(
            "activity component identity mismatch for {}",
            expected[700].0.group_id.key().0
        );
        let serial = serial_read(&connection, &wallets[0]).unwrap_err();
        assert!(
            serial.to_string().contains(&expected_message),
            "serial read reported {serial}"
        );
        for workers in WORKER_COUNTS {
            let scanned = scoped_with(workers, |scan| {
                scan.for_each(&connection, GENERATION, &wallets[0], |_, _| Ok(()))
                    .map(|_| ())
            })
            .unwrap_err();
            assert!(
                scanned.to_string().contains(&expected_message),
                "scan with {workers} worker(s) reported {scanned}"
            );
        }
    }

    #[test]
    fn an_earlier_decode_failure_precedes_a_later_unreadable_column() {
        let (connection, wallets) = seeded(&[600]);
        let expected = serial_read(&connection, &wallets[0]).unwrap();
        connection
            .execute(
                "UPDATE activity_groups_v2
                 SET components_json = json_set(components_json, '$.transaction_hash', 'changed')
                 WHERE source_trade_id = ?1",
                params![expected[10].0.group_id.key().0],
            )
            .unwrap();
        // A later row in the same wallet cannot even be extracted; the earlier
        // decode failure still owns the wallet, as the serial read shows.
        connection
            .execute(
                "UPDATE activity_groups_v2 SET components_json = CAST(components_json AS BLOB)
                 WHERE source_trade_id = ?1",
                params![expected[300].0.group_id.key().0],
            )
            .unwrap();
        let expected_message = format!(
            "activity component identity mismatch for {}",
            expected[10].0.group_id.key().0
        );
        let serial = serial_read(&connection, &wallets[0]).unwrap_err();
        assert!(
            serial.to_string().contains(&expected_message),
            "serial read reported {serial}"
        );
        for workers in WORKER_COUNTS {
            let scanned = scoped_with(workers, |scan| {
                scan.for_each(&connection, GENERATION, &wallets[0], |_, _| Ok(()))
                    .map(|_| ())
            })
            .unwrap_err();
            assert!(
                scanned.to_string().contains(&expected_message),
                "scan with {workers} worker(s) reported {scanned}"
            );
        }
    }

    #[test]
    fn an_unreadable_column_fails_the_wallet_when_no_row_fails_earlier() {
        let (connection, wallets) = seeded(&[600]);
        let expected = serial_read(&connection, &wallets[0]).unwrap();
        connection
            .execute(
                "UPDATE activity_groups_v2 SET components_json = CAST(components_json AS BLOB)
                 WHERE source_trade_id = ?1",
                params![expected[300].0.group_id.key().0],
            )
            .unwrap();
        let serial = serial_read(&connection, &wallets[0])
            .unwrap_err()
            .to_string();
        for workers in WORKER_COUNTS {
            let scanned = scoped_with(workers, |scan| {
                scan.for_each(&connection, GENERATION, &wallets[0], |_, _| Ok(()))
                    .map(|_| ())
            })
            .unwrap_err()
            .to_string();
            assert_eq!(scanned, serial, "scan with {workers} worker(s)");
        }
    }

    #[test]
    fn a_failed_wallet_leaves_the_pool_ready_for_the_next_one() {
        let (connection, wallets) = seeded(&[1_600, 1_600]);
        let poisoned = serial_read(&connection, &wallets[0]).unwrap();
        connection
            .execute(
                "UPDATE activity_groups_v2
                 SET components_json = json_set(components_json, '$.transaction_hash', 'changed')
                 WHERE source_trade_id = ?1",
                params![poisoned[3].0.group_id.key().0],
            )
            .unwrap();
        for workers in WORKER_COUNTS {
            let recovered = scoped_with(workers, |scan| {
                // Abandoning a wallet mid-flight must not leave batches queued.
                assert!(
                    scan.for_each(&connection, GENERATION, &wallets[0], |_, _| Ok(()))
                        .is_err()
                );
                read_wallet(scan, &connection, &wallets[1])
            })
            .unwrap();
            assert_matches_serial(&connection, &wallets[1], &recovered);
        }
    }

    /// A timestamp that decodes but cannot be written as RFC 3339, which the
    /// phased read only discovered after decoding and counting a whole wallet.
    const UNSERIALIZABLE_SECOND: i64 = -62_167_219_201;

    fn break_serialization(connection: &Connection, source_trade_id: &str) {
        connection
            .execute(
                "UPDATE activity_groups_v2 SET source_time_unix = ?2 WHERE source_trade_id = ?1",
                params![source_trade_id, UNSERIALIZABLE_SECOND],
            )
            .unwrap();
    }

    #[test]
    fn a_row_that_cannot_be_serialized_is_held_until_the_whole_wallet_is_read() {
        let (connection, wallets) = seeded(&[600]);
        let rows = serial_read(&connection, &wallets[0]).unwrap();
        break_serialization(&connection, &rows[10].0.group_id.key().0);
        let serial = serial_read(&connection, &wallets[0])
            .unwrap_err()
            .to_string();
        for workers in WORKER_COUNTS {
            let mut delivered = 0_usize;
            let held = scoped_with(workers, |scan| {
                scan.for_each(&connection, GENERATION, &wallets[0], |_, _| {
                    delivered += 1;
                    Ok(())
                })
            })
            .unwrap()
            .expect("the wallet has an unserializable row");
            // Every row is still delivered, so a caller can finish counting the
            // wallet before it reports this failure.
            assert_eq!(delivered, rows.len(), "{workers} worker(s) delivered rows");
            assert_eq!(held.to_string(), serial, "{workers} worker(s) reported");
        }
    }

    #[test]
    fn a_decode_failure_precedes_a_row_that_cannot_be_serialized() {
        let (connection, wallets) = seeded(&[600]);
        let rows = serial_read(&connection, &wallets[0]).unwrap();
        // The unserializable row sorts first; the phased read still reported
        // the decode failure, because it decoded before it serialized.
        break_serialization(&connection, &rows[10].0.group_id.key().0);
        connection
            .execute(
                "UPDATE activity_groups_v2
                 SET components_json = json_set(components_json, '$.transaction_hash', 'changed')
                 WHERE source_trade_id = ?1",
                params![rows[300].0.group_id.key().0],
            )
            .unwrap();
        let expected = format!(
            "activity component identity mismatch for {}",
            rows[300].0.group_id.key().0
        );
        let serial = serial_read(&connection, &wallets[0]).unwrap_err();
        assert!(
            serial.to_string().contains(&expected),
            "serial read reported {serial}"
        );
        for workers in WORKER_COUNTS {
            let scanned = scoped_with(workers, |scan| {
                scan.for_each(&connection, GENERATION, &wallets[0], |_, _| Ok(()))
                    .map(|_| ())
            })
            .unwrap_err();
            assert!(
                scanned.to_string().contains(&expected),
                "scan with {workers} worker(s) reported {scanned}"
            );
        }
    }

    #[test]
    fn an_invalid_utf8_column_reports_the_typed_read_error() {
        let (connection, wallets) = seeded(&[600]);
        let rows = serial_read(&connection, &wallets[0]).unwrap();
        connection
            .execute(
                "UPDATE activity_groups_v2 SET semantic_revision = CAST(x'80' AS TEXT)
                 WHERE source_trade_id = ?1",
                params![rows[42].0.group_id.key().0],
            )
            .unwrap();
        let serial = serial_read(&connection, &wallets[0])
            .unwrap_err()
            .to_string();
        for workers in WORKER_COUNTS {
            let scanned = scoped_with(workers, |scan| {
                scan.for_each(&connection, GENERATION, &wallets[0], |_, _| Ok(()))
                    .map(|_| ())
            })
            .unwrap_err()
            .to_string();
            assert_eq!(scanned, serial, "scan with {workers} worker(s)");
        }
    }

    #[test]
    fn rows_wider_than_a_batch_are_read_exactly() {
        let (connection, wallets) = seeded(&[40]);
        let rows = serial_read(&connection, &wallets[0]).unwrap();
        // Components the decoder ignores, each row wider than half a batch, so
        // admission ends a batch on bytes rather than rows.
        for row in rows.iter().take(6) {
            connection
                .execute(
                    "UPDATE activity_groups_v2
                     SET components_json = json_set(
                         components_json, '$.filler', replace(hex(zeroblob(1500000)), '0', 'x'))
                     WHERE source_trade_id = ?1",
                    params![row.0.group_id.key().0],
                )
                .unwrap();
        }
        for workers in WORKER_COUNTS {
            let read =
                scoped_with(workers, |scan| read_wallet(scan, &connection, &wallets[0])).unwrap();
            assert_matches_serial(&connection, &wallets[0], &read);
        }
    }

    #[test]
    fn a_consumer_that_stops_early_leaves_the_pool_ready() {
        let (connection, wallets) = seeded(&[5_200, 900]);
        for workers in WORKER_COUNTS {
            let recovered = scoped_with(workers, |scan| {
                let mut seen = 0_usize;
                assert!(
                    scan.for_each(&connection, GENERATION, &wallets[0], |_, _| {
                        seen += 1;
                        if seen == 5 {
                            return invalid("test stop".to_owned());
                        }
                        Ok(())
                    })
                    .is_err()
                );
                read_wallet(scan, &connection, &wallets[1])
            })
            .unwrap();
            assert_matches_serial(&connection, &wallets[1], &recovered);
        }
    }
}
