//! Advice only: private SQLite readers warm receipt pages without authenticating them.

use super::{FreshCollectionIdentity, generation_identity};
use rusqlite::{Connection, OpenFlags, blob::Blob};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

// Canonical defaults: docs/_GLOSSARY.md; these are not configuration keys.
const HISTORY_READ_AHEAD_CONNECTIONS: usize = 8;
const HISTORY_READ_AHEAD_CACHE_KIB: u64 = 2_048;
const HISTORY_READ_AHEAD_CHUNK_BYTES: usize = 65_536;
const HISTORY_READ_AHEAD_LEAD_GENERATIONS: usize = 8;

// In SQLite 3.51.3 a bare result-column IS NOT NULL materializes its input.
// CASE puts the same predicate in a branch, enabling TYPEOFARG.
const ROWIDS_SQL: &str = "SELECT rowid, CASE WHEN acquisition_json IS NOT NULL THEN 1 ELSE 0 END
    FROM activity_wallet_coverage_staging_v2 WHERE generation = ?1 ORDER BY wallet_hex";

pub(super) enum Outcome {
    Done,
    Cancelled,
    Unavailable(String),
}

impl Outcome {
    pub(super) fn status(&self) -> &'static str {
        match self {
            Self::Done => "done",
            Self::Cancelled => "cancelled",
            Self::Unavailable(_) => "unavailable",
        }
    }

    pub(super) fn error(&self) -> Option<&str> {
        match self {
            Self::Unavailable(error) => Some(error),
            Self::Done | Self::Cancelled => None,
        }
    }
}

pub(super) struct Report {
    pub(super) outcome: Outcome,
    pub(super) budget_bytes: Option<u64>,
}

pub(super) struct Progress {
    stop: Arc<AtomicBool>,
    completed: AtomicUsize,
    next: AtomicUsize,
    gate: Mutex<()>,
    wake: Condvar,
    hooks: Hooks,
}

impl Progress {
    fn new(hooks: Hooks) -> Self {
        Self {
            stop: Arc::new(AtomicBool::new(false)),
            completed: AtomicUsize::new(0),
            next: AtomicUsize::new(0),
            gate: Mutex::new(()),
            wake: Condvar::new(),
            hooks,
        }
    }

    pub(super) fn advance(&self) {
        self.completed.fetch_add(1, Ordering::Relaxed);
        self.wake.notify_all();
    }

    fn cancel(&self) {
        self.stop.store(true, Ordering::Relaxed);
        self.wake.notify_all();
    }

    fn point(&self, worker: usize, point: Point) -> rusqlite::Result<()> {
        self.hooks.point(worker, point, &self.stop)
    }

    fn wait(&self, worker: usize, index: usize) -> rusqlite::Result<bool> {
        let mut gate = self
            .gate
            .lock()
            .map_err(|_| rusqlite::Error::InvalidQuery)?;
        while !self.stop.load(Ordering::Relaxed)
            && index
                >= self
                    .completed
                    .load(Ordering::Relaxed)
                    .saturating_add(HISTORY_READ_AHEAD_LEAD_GENERATIONS)
        {
            self.point(worker, Point::Waiting)?;
            gate = self
                .wake
                .wait_timeout(gate, Duration::from_millis(10))
                .map_err(|_| rusqlite::Error::InvalidQuery)?
                .0;
        }
        Ok(!self.stop.load(Ordering::Relaxed))
    }
}

struct StopOnDrop<'a>(&'a Progress);

impl Drop for StopOnDrop<'_> {
    fn drop(&mut self) {
        self.0.cancel();
        let _ = self.0.point(0, Point::Stopping);
    }
}

// SQLite's accessPayload allocates nOvfl * 2 * sizeof(Pgno) per cursor, retained
// across blob_reopen. MAX_LENGTH bounds the whole row, not each JSON column.
fn budget(page_size: u64) -> Result<u64, String> {
    if !(512..=65_536).contains(&page_size) || !page_size.is_power_of_two() {
        return Err(format!("invalid read-ahead page_size {page_size}"));
    }
    let cursor = 1_000_000_000_u64.div_ceil(page_size - 4) * 2 * 4;
    Ok(
        u64::try_from(HISTORY_READ_AHEAD_CONNECTIONS).map_err(|e| e.to_string())?
            * (HISTORY_READ_AHEAD_CACHE_KIB * 1024
                + u64::try_from(HISTORY_READ_AHEAD_CHUNK_BYTES).map_err(|e| e.to_string())?
                + 3 * cursor),
    )
}

fn open(
    path: &str,
    progress: &Progress,
    worker: usize,
    interrupted_for_stop: Arc<AtomicBool>,
) -> rusqlite::Result<Connection> {
    let connection = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    let stop = Arc::clone(&progress.stop);
    let hooks = progress.hooks.clone();
    connection.progress_handler(
        64,
        Some(move || {
            let failed = hooks.point(worker, Point::Progress, &stop).is_err();
            let cancelled = stop.load(Ordering::Relaxed);
            if cancelled {
                interrupted_for_stop.store(true, Ordering::Relaxed);
            }
            failed || cancelled
        }),
    )?;
    connection.busy_timeout(Duration::from_millis(50))?;
    connection.pragma_update(
        None,
        "cache_size",
        -i64::try_from(HISTORY_READ_AHEAD_CACHE_KIB).map_err(|_| rusqlite::Error::InvalidQuery)?,
    )?;
    Ok(connection)
}

fn hint(
    path: &str,
    head: &FreshCollectionIdentity,
    progress: &Progress,
) -> Result<Vec<u64>, String> {
    let connection = open(path, progress, usize::MAX, Arc::new(AtomicBool::new(false)))
        .map_err(|e| format!("open history hint: {e}"))?;
    let _connection = progress.hooks.resource(usize::MAX, Resource::Connection);
    let mut identity = head.clone();
    let mut generations = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    loop {
        if !seen.insert(identity.generation) {
            return Err("cycle in history read-ahead hint".to_owned());
        }
        generations.try_reserve(1).map_err(|e| e.to_string())?;
        generations.push(identity.generation);
        let Some(base) = identity.base_generation else {
            break;
        };
        identity = generation_identity(&connection, base)
            .map_err(|e| format!("history hint identity: {e}"))?
            .ok_or_else(|| "history hint identity missing".to_owned())?;
    }
    Ok(generations)
}

pub(super) fn run<T>(
    connection: &Connection,
    head: &FreshCollectionIdentity,
    verify: impl FnOnce(&Progress) -> T,
) -> (T, Report) {
    let hooks = Hooks::current();
    let progress = Progress::new(hooks);
    let budget_bytes = connection
        .pragma_query_value(None, "page_size", |row| row.get(0))
        .map_err(|e| format!("history read-ahead page_size: {e}"))
        .and_then(budget);
    let setup = (|| {
        progress.point(0, Point::Setup).map_err(|e| e.to_string())?;
        let path = connection
            .path()
            .filter(|path| !path.is_empty())
            .ok_or_else(|| "history read-ahead requires a file-backed database".to_owned())?;
        budget_bytes.as_ref().map_err(Clone::clone)?;
        Ok::<_, String>((path, hint(path, head, &progress)?))
    })();
    let (path, generations) = match setup {
        Ok(setup) => setup,
        Err(error) => {
            return (
                verify(&progress),
                Report {
                    outcome: Outcome::Unavailable(error),
                    budget_bytes: budget_bytes.ok(),
                },
            );
        }
    };
    thread::scope(|scope| {
        let mut workers = Vec::new();
        if let Err(error) = workers.try_reserve_exact(HISTORY_READ_AHEAD_CONNECTIONS) {
            return (
                verify(&progress),
                Report {
                    outcome: Outcome::Unavailable(format!(
                        "reserve history read-ahead workers: {error}"
                    )),
                    budget_bytes: budget_bytes.ok(),
                },
            );
        }
        let mut failure = None;
        for worker in 0..HISTORY_READ_AHEAD_CONNECTIONS.min(generations.len()) {
            let progress = &progress;
            let generations = &generations;
            let spawn = progress
                .point(worker, Point::Spawn)
                .map_err(|e| e.to_string())
                .and_then(|()| {
                    thread::Builder::new()
                        .name(format!("history-read-ahead-{worker}"))
                        .spawn_scoped(scope, move || {
                            let outcome = warm(path, generations, progress, worker);
                            if matches!(outcome, Outcome::Unavailable(_)) {
                                progress.cancel();
                            }
                            outcome
                        })
                        .map_err(|e| format!("spawn history read-ahead: {e}"))
                });
            match spawn {
                Ok(handle) => workers.push(handle),
                Err(error) => {
                    failure = Some(error);
                    progress.cancel();
                    break;
                }
            }
        }
        let stop = StopOnDrop(&progress);
        let _ = progress.point(0, Point::Verifier);
        let result = verify(&progress);
        drop(stop);
        let mut cancelled = false;
        for worker in workers {
            match worker.join() {
                Ok(Outcome::Done) => {}
                Ok(Outcome::Cancelled) => cancelled = true,
                Ok(Outcome::Unavailable(error)) => {
                    if failure.is_none() {
                        failure = Some(error);
                    }
                }
                Err(_) => {
                    if failure.is_none() {
                        failure = Some("history read-ahead worker panicked".to_owned());
                    }
                }
            }
        }
        let outcome = match failure {
            Some(error) => Outcome::Unavailable(error),
            None if cancelled => Outcome::Cancelled,
            None => Outcome::Done,
        };
        (
            result,
            Report {
                outcome,
                budget_bytes: budget_bytes.ok(),
            },
        )
    })
}

fn warm(path: &str, generations: &[u64], progress: &Progress, worker: usize) -> Outcome {
    let _thread = progress.hooks.resource(worker, Resource::Thread);
    let interrupted_for_stop = Arc::new(AtomicBool::new(false));
    let result = (|| {
        progress.point(worker, Point::Open)?;
        let connection = open(path, progress, worker, Arc::clone(&interrupted_for_stop))?;
        let _connection = progress.hooks.resource(worker, Resource::Connection);
        let mut buffer = Vec::new();
        buffer
            .try_reserve_exact(HISTORY_READ_AHEAD_CHUNK_BYTES)
            .map_err(|_| rusqlite::Error::InvalidQuery)?;
        buffer.resize(HISTORY_READ_AHEAD_CHUNK_BYTES, 0);
        let _buffer = progress.hooks.resource(worker, Resource::Buffer);
        progress.point(worker, Point::Prepare)?;
        let mut statement = connection.prepare(ROWIDS_SQL)?;
        let _statement = progress.hooks.resource(worker, Resource::Statement);
        let mut pages = None;
        let mut acquisition = None;
        loop {
            let index = progress.next.fetch_add(1, Ordering::Relaxed);
            let Some(&generation) = generations.get(index) else {
                return Ok(true);
            };
            if !progress.wait(worker, index)? {
                return Ok(false);
            }
            progress.point(
                worker,
                Point::Generation {
                    index,
                    completed: progress.completed.load(Ordering::Relaxed),
                },
            )?;
            let mut rows = statement.query([generation])?;
            loop {
                if progress.stop.load(Ordering::Relaxed) {
                    return Ok(false);
                }
                progress.point(worker, Point::Step)?;
                let Some(row) = rows.next()? else { break };
                let rowid = row.get(0)?;
                let has_acquisition = row.get::<_, bool>(1)?;
                progress.point(
                    worker,
                    Point::Row {
                        rowid,
                        has_acquisition,
                    },
                )?;
                if !read_column(
                    &connection,
                    &mut pages,
                    "page_evidence_json",
                    rowid,
                    &mut buffer,
                    progress,
                    worker,
                )? {
                    return Ok(false);
                }
                if has_acquisition
                    && !read_column(
                        &connection,
                        &mut acquisition,
                        "acquisition_json",
                        rowid,
                        &mut buffer,
                        progress,
                        worker,
                    )?
                {
                    return Ok(false);
                }
            }
        }
    })();
    let outcome = match result {
        Ok(true) => Outcome::Done,
        Ok(false) => Outcome::Cancelled,
        Err(rusqlite::Error::SqliteFailure(error, _))
            if error.code == rusqlite::ErrorCode::OperationInterrupted
                && interrupted_for_stop.load(Ordering::Relaxed) =>
        {
            Outcome::Cancelled
        }
        Err(error) => Outcome::Unavailable(format!("history read-ahead worker {worker}: {error}")),
    };
    let _ = progress.point(worker, Point::Finished);
    outcome
}

struct Column<'a> {
    blob: Blob<'a>,
    _resource: ResourceGuard,
}

fn read_column<'a>(
    connection: &'a Connection,
    column: &mut Option<Column<'a>>,
    name: &str,
    rowid: i64,
    buffer: &mut [u8],
    progress: &Progress,
    worker: usize,
) -> rusqlite::Result<bool> {
    progress.point(worker, Point::Blob)?;
    match column {
        Some(column) => {
            column.blob.reopen(rowid)?;
        }
        None => {
            *column = Some(Column {
                blob: connection.blob_open(
                    "main",
                    "activity_wallet_coverage_staging_v2",
                    name,
                    rowid,
                    true,
                )?,
                _resource: progress.hooks.resource(worker, Resource::Blob),
            })
        }
    }
    if let Some(column) = column {
        let mut offset = 0;
        while offset < column.blob.len() {
            if progress.stop.load(Ordering::Relaxed) {
                return Ok(false);
            }
            let count = buffer.len().min(column.blob.len() - offset);
            column.blob.read_at_exact(&mut buffer[..count], offset)?;
            offset += count;
            progress.point(worker, Point::Chunk { bytes: count })?;
        }
    }
    Ok(true)
}

#[derive(Clone, Copy, Debug)]
#[cfg_attr(not(any(test, feature = "scenario")), allow(dead_code))]
pub enum Resource {
    Thread,
    Connection,
    Statement,
    Blob,
    Buffer,
}

#[derive(Clone, Copy, Debug)]
#[cfg_attr(not(any(test, feature = "scenario")), allow(dead_code))]
pub enum Point {
    Setup,
    Spawn,
    Open,
    Prepare,
    Step,
    Blob,
    Progress,
    Waiting,
    Verifier,
    Finished,
    Stopping,
    Generation { index: usize, completed: usize },
    Chunk { bytes: usize },
    Row { rowid: i64, has_acquisition: bool },
}

#[cfg(not(any(test, feature = "scenario")))]
#[derive(Clone, Default)]
struct Hooks;

#[cfg(not(any(test, feature = "scenario")))]
impl Hooks {
    fn current() -> Self {
        Self
    }
    fn point(&self, _: usize, _: Point, _: &AtomicBool) -> rusqlite::Result<()> {
        Ok(())
    }
    fn resource(&self, _: usize, _: Resource) -> ResourceGuard {
        ResourceGuard
    }
}

#[cfg(not(any(test, feature = "scenario")))]
struct ResourceGuard;

#[cfg(any(test, feature = "scenario"))]
mod test_hooks;
#[cfg(all(test, not(feature = "scenario")))]
use test_hooks::{HistoryReadAheadAction, HistoryReadAheadEvent};
#[cfg(feature = "scenario")]
pub use test_hooks::{
    HistoryReadAheadAction, HistoryReadAheadEvent, set_history_read_ahead_hook_for_test,
};
#[cfg(any(test, feature = "scenario"))]
use test_hooks::{Hooks, ResourceGuard};

#[cfg(test)]
mod tests;
