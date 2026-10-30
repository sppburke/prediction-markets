use std::collections::HashMap;
use std::fs::File;
use std::num::NonZeroU64;
use std::os::unix::fs::FileExt as _;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::Duration;

use rusqlite::Connection;
use rustix::fs::{Advice, fadvise};

// Canonical defaults: docs/_GLOSSARY.md. The kernel reads at most max(io_pages, ra_pages)
// of one WILLNEED call, so a chunk stays within the default 128 KiB read-ahead window.
const LEAD_BYTES: u64 = 1 << 30;
const PROGRESS_STEP_PAGES: u16 = 256;
const ADVICE_CHUNK_BYTES: u64 = 128 << 10;
const SPINE_LIMIT: usize = 20;

pub(super) enum Outcome {
    Done,
    Cancelled,
    Unavailable(String),
}

impl Outcome {
    pub(super) fn unavailable(error: impl Into<String>) -> Self {
        Self::Unavailable(error.into())
    }

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

struct Setup {
    file: File,
    io: File,
    roots: Vec<u32>,
    scan_roots: Vec<u32>,
    page_size: u64,
    page_count: u64,
}

impl Setup {
    fn new(connection: &Connection, path: &Path) -> Result<Self, String> {
        let mut statement = connection
            .prepare("EXPLAIN PRAGMA quick_check")
            .map_err(|error| format!("explain quick_check: {error}"))?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, i64>(6)?,
                ))
            })
            .map_err(|error| format!("explain quick_check rows: {error}"))?;
        let mut roots = None;
        let mut cursors = HashMap::new();
        let mut scan_roots = Vec::new();
        for row in rows {
            let (opcode, p1, p2, p3, p4, p5) =
                row.map_err(|error| format!("explain row: {error}"))?;
            if opcode == "IntegrityCk" && p5 == 0 {
                let p4 = p4.ok_or("IntegrityCk has no root list")?;
                let parsed = p4
                    .trim_start_matches('[')
                    .trim_end_matches(']')
                    .split(',')
                    .map(|part| {
                        part.trim()
                            .parse::<u32>()
                            .map_err(|error| format!("invalid IntegrityCk root {part:?}: {error}"))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                roots = Some(parsed);
            } else if opcode == "OpenRead" {
                cursors.remove(&p1);
                if p3 == 0 {
                    let root = u32::try_from(p2)
                        .map_err(|error| format!("invalid OpenRead root {p2}: {error}"))?;
                    cursors.insert(p1, root);
                }
            } else if opcode == "Rewind" {
                if let Some(&root) = cursors.get(&p1) {
                    scan_roots.push(root);
                }
            } else if opcode == "Close" {
                cursors.remove(&p1);
            }
        }
        let roots = roots.ok_or("main-schema IntegrityCk not found")?;
        let page_size: u64 = connection
            .pragma_query_value(None, "page_size", |row| row.get(0))
            .map_err(|error| format!("page_size: {error}"))?;
        if !(512..=65_536).contains(&page_size) || !page_size.is_power_of_two() {
            return Err(format!("invalid page_size {page_size}"));
        }
        let file = File::open(path).map_err(|error| format!("open cache: {error}"))?;
        let bytes = file
            .metadata()
            .map_err(|error| format!("cache size: {error}"))?
            .len();
        if bytes == 0 || bytes % page_size != 0 {
            return Err(format!("cache size {bytes} is not a whole number of pages"));
        }
        let io = File::open("/proc/thread-self/io")
            .map_err(|error| format!("open checking thread io: {error}"))?;
        Ok(Self {
            file,
            io,
            roots,
            scan_roots,
            page_size,
            page_count: bytes / page_size,
        })
    }
}

struct StopOnDrop<'a>(&'a AtomicBool);

impl Drop for StopOnDrop<'_> {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

struct ProgressOnDrop<'a>(&'a Connection);

impl Drop for ProgressOnDrop<'_> {
    fn drop(&mut self) {
        // The setter fails only for a borrowed handle; rusqlite opened this connection.
        let _ = self.0.progress_handler(0, None::<fn() -> bool>);
    }
}

pub(super) fn quick_check(
    connection: &Connection,
    path: &Path,
) -> (rusqlite::Result<String>, Outcome) {
    quick_check_inner(connection, path, None, Arc::new(AtomicU64::new(0)))
}

fn quick_check_inner(
    connection: &Connection,
    path: &Path,
    limits: Option<(u64, u64)>,
    counter: Arc<AtomicU64>,
) -> (rusqlite::Result<String>, Outcome) {
    let setup = match Setup::new(connection, path) {
        Ok(setup) => setup,
        Err(error) => {
            return (
                connection.query_row("PRAGMA quick_check", [], |row| row.get(0)),
                Outcome::Unavailable(error),
            );
        }
    };
    let (lead, chunk) = limits.unwrap_or((
        LEAD_BYTES / setup.page_size,
        ADVICE_CHUNK_BYTES / setup.page_size,
    ));
    let mut statement = match connection.prepare("PRAGMA quick_check") {
        Ok(statement) => statement,
        Err(error) => {
            return (
                Err(error),
                Outcome::unavailable("prepare quick_check failed"),
            );
        }
    };
    let stop = AtomicBool::new(false);
    thread::scope(|scope| {
        let progress = Arc::clone(&counter);
        // The setter fails only for a borrowed handle; rusqlite opened this connection.
        let _ = connection.progress_handler(
            i32::from(PROGRESS_STEP_PAGES),
            Some(move || {
                progress.fetch_add(u64::from(PROGRESS_STEP_PAGES), Ordering::Relaxed);
                false
            }),
        );
        let progress_guard = ProgressOnDrop(connection);
        let stop_guard = StopOnDrop(&stop);
        let worker = thread::Builder::new()
            .name("quick-check-prefetch".to_owned())
            .spawn_scoped(scope, || run_worker(&setup, &stop, &counter, lead, chunk));
        let result = statement.query_row([], |row| row.get(0));
        drop(progress_guard);
        drop(stop_guard);
        let outcome = match worker {
            Ok(worker) => match worker.join() {
                Ok(outcome) => outcome,
                Err(_) => Outcome::unavailable("prefetch worker panicked"),
            },
            Err(error) => Outcome::unavailable(format!("spawn prefetch worker: {error}")),
        };
        (result, outcome)
    })
}

fn read_syscr(io: &File) -> Result<u64, String> {
    let mut buffer = [0_u8; 4096];
    let len = io
        .read_at(&mut buffer, 0)
        .map_err(|error| format!("read checking thread io: {error}"))?;
    let contents = std::str::from_utf8(&buffer[..len])
        .map_err(|error| format!("checking thread io is not UTF-8: {error}"))?;
    let value = contents
        .lines()
        .find_map(|line| line.strip_prefix("syscr:"))
        .ok_or("checking thread io has no syscr")?;
    value
        .trim()
        .parse()
        .map_err(|error| format!("invalid syscr: {error}"))
}

fn advice_pages(advised: u64, consumed: u64, lead: u64, chunk: u64) -> u64 {
    consumed
        .saturating_add(lead)
        .saturating_sub(advised)
        .min(chunk)
}

fn walk_finished(counter: u64, walk_total: u64) -> bool {
    counter.saturating_add(u64::from(PROGRESS_STEP_PAGES)) > walk_total
}

#[derive(Debug)]
enum WorkError {
    Cancelled,
    Unavailable(String),
}

type WorkResult<T> = Result<T, WorkError>;

fn check_stop(stop: &AtomicBool) -> WorkResult<()> {
    if stop.load(Ordering::Relaxed) {
        Err(WorkError::Cancelled)
    } else {
        Ok(())
    }
}

fn run_worker(
    setup: &Setup,
    stop: &AtomicBool,
    counter: &AtomicU64,
    lead: u64,
    chunk: u64,
) -> Outcome {
    let mut adviser = Adviser {
        setup,
        stop,
        advised: 0,
        counter,
        lead,
        chunk,
        scan: None,
        direction: Direction::Descending,
        run_first: None,
        run_len: 0,
    };
    let result = (|| {
        let mut walk_total = 0;
        walk(setup, stop, |page| {
            walk_total += 1;
            adviser.push(page)
        })?;
        adviser.flush()?;
        adviser.start_scan(walk_total)?;
        for &root in &setup.scan_roots {
            preorder(setup, root, stop, true, &mut |page| adviser.push(page))?;
        }
        adviser.flush()
    })();
    match result {
        Ok(()) => Outcome::Done,
        Err(WorkError::Cancelled) => Outcome::Cancelled,
        Err(WorkError::Unavailable(error)) => Outcome::Unavailable(error),
    }
}

#[derive(Clone, Copy)]
enum Direction {
    Descending,
    Ascending,
}

struct Adviser<'a> {
    setup: &'a Setup,
    stop: &'a AtomicBool,
    advised: u64,
    counter: &'a AtomicU64,
    lead: u64,
    chunk: u64,
    scan: Option<(u64, u64)>,
    direction: Direction,
    run_first: Option<u32>,
    run_len: u64,
}

impl Adviser<'_> {
    fn start_scan(&mut self, walk_total: u64) -> WorkResult<()> {
        while !walk_finished(self.counter.load(Ordering::Relaxed), walk_total) {
            check_stop(self.stop)?;
            thread::sleep(Duration::from_millis(1));
        }
        check_stop(self.stop)?;
        let base = read_syscr(&self.setup.io).map_err(WorkError::Unavailable)?;
        self.scan = Some((walk_total, base));
        self.direction = Direction::Ascending;
        Ok(())
    }

    fn allowance(&self) -> WorkResult<u64> {
        let consumed = match self.scan {
            None => self.counter.load(Ordering::Relaxed),
            Some((walk_total, base)) => {
                // Scan pacing is a heuristic: overflow and header reads also count.
                walk_total.saturating_add(
                    read_syscr(&self.setup.io)
                        .map_err(WorkError::Unavailable)?
                        .saturating_sub(base),
                )
            }
        };
        Ok(advice_pages(self.advised, consumed, self.lead, self.chunk))
    }

    fn push(&mut self, page: u32) -> WorkResult<()> {
        check_stop(self.stop)?;
        if let Some(first) = self.run_first {
            let expected = match self.direction {
                Direction::Descending => u64::from(first).saturating_sub(self.run_len),
                Direction::Ascending => u64::from(first) + self.run_len,
            };
            if u64::from(page) == expected && self.run_len < self.chunk {
                self.run_len += 1;
                return Ok(());
            }
            self.flush()?;
        }
        self.run_first = Some(page);
        self.run_len = 1;
        Ok(())
    }

    fn flush(&mut self) -> WorkResult<()> {
        let Some(first) = self.run_first.take() else {
            return Ok(());
        };
        let mut remaining = self.run_len;
        let mut first = u64::from(first);
        self.run_len = 0;
        while remaining > 0 {
            check_stop(self.stop)?;
            let mut allowed = self.allowance()?;
            if allowed == 0 {
                loop {
                    check_stop(self.stop)?;
                    allowed = self.allowance()?;
                    if allowed >= self.chunk {
                        break;
                    }
                    thread::sleep(Duration::from_millis(1));
                }
            }
            let take = remaining.min(allowed);
            let low = match self.direction {
                Direction::Descending => first - take,
                Direction::Ascending => first - 1,
            };
            let offset = low * self.setup.page_size;
            let bytes = take * self.setup.page_size;
            let len = NonZeroU64::new(bytes)
                .ok_or_else(|| WorkError::Unavailable("zero advice length".to_owned()))?;
            check_stop(self.stop)?;
            fadvise(&self.setup.file, offset, Some(len), Advice::WillNeed)
                .map_err(|error| WorkError::Unavailable(format!("fadvise: {error}")))?;
            self.advised += take;
            remaining -= take;
            match self.direction {
                Direction::Descending => first -= take,
                Direction::Ascending => first += take,
            }
        }
        Ok(())
    }
}

fn page(setup: &Setup, number: u32, stop: &AtomicBool) -> WorkResult<Vec<u8>> {
    check_stop(stop)?;
    if number == 0 || u64::from(number) > setup.page_count {
        return Err(WorkError::Unavailable(format!(
            "page {number} is out of range"
        )));
    }
    let size = usize::try_from(setup.page_size)
        .map_err(|error| WorkError::Unavailable(format!("page size: {error}")))?;
    let mut bytes = vec![0; size];
    setup
        .file
        .read_exact_at(&mut bytes, (u64::from(number) - 1) * setup.page_size)
        .map_err(|error| WorkError::Unavailable(format!("read page {number}: {error}")))?;
    Ok(bytes)
}

fn field<const N: usize>(bytes: &[u8], offset: usize) -> WorkResult<[u8; N]> {
    bytes
        .get(offset..offset.saturating_add(N))
        .and_then(|slice| slice.try_into().ok())
        .ok_or_else(|| WorkError::Unavailable(format!("page field at {offset} is out of range")))
}

fn kind(bytes: &[u8], number: u32) -> WorkResult<u8> {
    let header = if number == 1 { 100 } else { 0 };
    let kind = field::<1>(bytes, header)?[0];
    if !matches!(kind, 0x02 | 0x05 | 0x0a | 0x0d) {
        return Err(WorkError::Unavailable(format!(
            "page {number} has invalid b-tree type {kind}"
        )));
    }
    let header_size = if matches!(kind, 0x02 | 0x05) { 12 } else { 8 };
    field::<1>(bytes, header + header_size - 1)?;
    let count = usize::from(u16::from_be_bytes(field::<2>(bytes, header + 3)?));
    if header + header_size + count * 2 > bytes.len() {
        return Err(WorkError::Unavailable(format!(
            "page {number} cell pointers exceed page"
        )));
    }
    Ok(kind)
}

fn children(bytes: &[u8], number: u32, page_count: u64) -> WorkResult<Vec<u32>> {
    let header = if number == 1 { 100 } else { 0 };
    let kind = kind(bytes, number)?;
    if !matches!(kind, 0x02 | 0x05) {
        return Err(WorkError::Unavailable(format!(
            "page {number} is not interior"
        )));
    }
    let count = usize::from(u16::from_be_bytes(field::<2>(bytes, header + 3)?));
    let pointers_end = header + 12 + count * 2;
    if pointers_end > bytes.len() {
        return Err(WorkError::Unavailable(format!(
            "page {number} cell pointers exceed page"
        )));
    }
    let mut result = Vec::with_capacity(count + 1);
    for index in 0..count {
        let offset = usize::from(u16::from_be_bytes(field::<2>(
            bytes,
            header + 12 + index * 2,
        )?));
        if offset < pointers_end {
            return Err(WorkError::Unavailable(format!(
                "page {number} cell {index} has invalid offset"
            )));
        }
        let child = u32::from_be_bytes(field::<4>(bytes, offset).map_err(|_| {
            WorkError::Unavailable(format!("page {number} cell {index} exceeds page"))
        })?);
        if child == 0 || u64::from(child) > page_count {
            return Err(WorkError::Unavailable(format!(
                "page {number} child {child} is out of range"
            )));
        }
        result.push(child);
    }
    let right = u32::from_be_bytes(field::<4>(bytes, header + 8)?);
    if right == 0 || u64::from(right) > page_count {
        return Err(WorkError::Unavailable(format!(
            "page {number} right child {right} is out of range"
        )));
    }
    result.push(right);
    Ok(result)
}

fn walk(
    setup: &Setup,
    stop: &AtomicBool,
    mut emit: impl FnMut(u32) -> WorkResult<()>,
) -> WorkResult<()> {
    for &root in &setup.roots {
        check_stop(stop)?;
        if root != 0 {
            preorder(setup, root, stop, false, &mut emit)?;
        }
    }
    Ok(())
}

fn leaf_depth(setup: &Setup, root: u32, stop: &AtomicBool) -> WorkResult<usize> {
    let mut spine = root;
    for level in 0..SPINE_LIMIT {
        let bytes = page(setup, spine, stop)?;
        if matches!(kind(&bytes, spine)?, 0x0a | 0x0d) {
            return Ok(level);
        }
        let header = if spine == 1 { 100 } else { 0 };
        let right = u32::from_be_bytes(field::<4>(&bytes, header + 8)?);
        if right == 0 || u64::from(right) > setup.page_count {
            return Err(WorkError::Unavailable(format!(
                "page {spine} right child {right} is out of range"
            )));
        }
        spine = right;
    }
    Err(WorkError::Unavailable(
        "right spine exceeds 20 levels".to_owned(),
    ))
}

// Pre-order over one b-tree, emitting every page in the order SQLite reads it: right to left in
// its walk, left to right in its row scan. Only interior pages are read here.
fn preorder(
    setup: &Setup,
    root: u32,
    stop: &AtomicBool,
    left_to_right: bool,
    emit: &mut impl FnMut(u32) -> WorkResult<()>,
) -> WorkResult<()> {
    let depth = leaf_depth(setup, root, stop)?;
    let mut stack = vec![(root, 0)];
    while let Some((number, level)) = stack.pop() {
        check_stop(stop)?;
        emit(number)?;
        if level == depth {
            continue;
        }
        let bytes = page(setup, number, stop)?;
        let mut next = children(&bytes, number, setup.page_count)?;
        if left_to_right {
            next.reverse();
        }
        stack.extend(next.into_iter().map(|child| (child, level + 1)));
    }
    Ok(())
}

#[cfg(test)]
mod bench;

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::cmp::Ordering as CmpOrdering;
    use std::collections::BTreeSet;
    use std::io::{Seek as _, SeekFrom, Write as _};

    use rusqlite::{OpenFlags, params};

    use super::*;

    fn empty_fixture() -> (tempfile::TempDir, std::path::PathBuf, Connection) {
        let target = std::env::var_os("CARGO_TARGET_DIR").map_or_else(
            || std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target"),
            std::path::PathBuf::from,
        );
        std::fs::create_dir_all(&target).unwrap();
        let dir = tempfile::tempdir_in(target).unwrap();
        let path = dir.path().join("walk.db");
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch("PRAGMA page_size=8192; PRAGMA auto_vacuum=INCREMENTAL;")
            .unwrap();
        (dir, path, connection)
    }

    fn fixture() -> (tempfile::TempDir, std::path::PathBuf, Connection) {
        let (dir, path, mut connection) = empty_fixture();
        connection
            .execute_batch(
                "CREATE TABLE payload (id INTEGER PRIMARY KEY, key TEXT NOT NULL, data TEXT NOT NULL);
            CREATE INDEX payload_key ON payload(key);",
            )
            .unwrap();
        let transaction = connection.transaction().unwrap();
        for number in 0..5_000 {
            let key = format!("{:08}-{}", number, "k".repeat(100));
            let data = if number == 100 {
                "v".repeat(20_000)
            } else {
                "v".repeat(100)
            };
            transaction
                .execute(
                    "INSERT INTO payload (key, data) VALUES (?1, ?2)",
                    params![key, data],
                )
                .unwrap();
        }
        transaction.commit().unwrap();
        for number in 0..197 {
            connection
                .execute_batch(&format!(
                    "CREATE TABLE aux_{number:03} (id INTEGER PRIMARY KEY, note TEXT DEFAULT '{}');",
                    "n".repeat(80)
                ))
                .unwrap();
        }
        assert_eq!(
            connection
                .pragma_query_value::<i64, _>(None, "page_size", |row| row.get(0))
                .unwrap(),
            8192
        );
        assert_eq!(
            connection
                .pragma_query_value::<i64, _>(None, "auto_vacuum", |row| row.get(0))
                .unwrap(),
            2
        );
        let interior: i64 = connection.query_row(
            "SELECT count(*) FROM dbstat WHERE name IN ('payload', 'payload_key') AND pagetype = 'internal'",
            [], |row| row.get(0),
        ).unwrap();
        assert!(interior >= 2);
        let schema_type: String = connection
            .query_row("SELECT pagetype FROM dbstat WHERE pageno = 1", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(schema_type, "internal");
        let overflow: i64 = connection
            .query_row(
                "SELECT count(*) FROM dbstat WHERE pagetype = 'overflow'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(overflow > 0);
        (dir, path, connection)
    }

    fn path_order(left: &str, right: &str) -> CmpOrdering {
        let left = left.trim_matches('/');
        let right = right.trim_matches('/');
        let mut left = left.split('/').filter(|part| !part.is_empty());
        let mut right = right.split('/').filter(|part| !part.is_empty());
        loop {
            match (left.next(), right.next()) {
                (None, None) => return CmpOrdering::Equal,
                (None, Some(_)) => return CmpOrdering::Less,
                (Some(_), None) => return CmpOrdering::Greater,
                (Some(a), Some(b)) if a != b => return b.cmp(a),
                _ => {}
            }
        }
    }

    fn sqlite_progress(connection: &Connection) -> (String, Arc<AtomicU64>) {
        let counter = Arc::new(AtomicU64::new(0));
        let progress = Arc::clone(&counter);
        let mut statement = connection.prepare("PRAGMA quick_check").unwrap();
        connection
            .progress_handler(
                256,
                Some(move || {
                    progress.fetch_add(u64::from(PROGRESS_STEP_PAGES), Ordering::Relaxed);
                    false
                }),
            )
            .unwrap();
        let verdict = statement.query_row([], |row| row.get(0)).unwrap();
        connection
            .progress_handler(0, None::<fn() -> bool>)
            .unwrap();
        (verdict, counter)
    }

    #[test]
    fn worker_walk_matches_dbstat_and_sqlite_verdict() {
        let (_dir, path, connection) = fixture();
        let setup = Setup::new(&connection, &path).unwrap();
        let stop = AtomicBool::new(false);
        let mut emitted = Vec::new();
        walk(&setup, &stop, |page| {
            emitted.push(page);
            Ok(())
        })
        .unwrap();
        let mut expected = Vec::new();
        for root in setup.roots.iter().copied().filter(|root| *root != 0) {
            let name: String = connection
                .query_row(
                    "SELECT name FROM dbstat WHERE pageno = ?1 AND path = '/'",
                    [root],
                    |row| row.get(0),
                )
                .unwrap();
            let mut rows = connection
                .prepare(
                    "SELECT path, pageno FROM dbstat WHERE name = ?1 AND pagetype != 'overflow'",
                )
                .unwrap();
            let mut pages = rows
                .query_map([name], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, u32>(1)?))
                })
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap();
            pages.sort_by(|a, b| path_order(&a.0, &b.0));
            expected.extend(pages.into_iter().map(|(_, page)| page));
        }
        assert_eq!(emitted, expected);
        assert_eq!(setup.roots.len(), 200);
        let tree_pages: u64 = connection
            .query_row(
                "SELECT count(*) FROM dbstat WHERE pagetype != 'overflow'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(u64::try_from(emitted.len()).unwrap(), tree_pages);
        let (plain, counter) = sqlite_progress(&connection);
        assert_eq!(plain, "ok");
        assert!(walk_finished(counter.load(Ordering::Relaxed), tree_pages));
        assert!(matches!(
            run_worker(
                &setup,
                &stop,
                &counter,
                LEAD_BYTES / setup.page_size,
                ADVICE_CHUNK_BYTES / setup.page_size,
            ),
            Outcome::Done
        ));
        let (prefetched, _) = quick_check(&connection, &path);
        assert_eq!(prefetched.unwrap(), plain);
    }

    #[test]
    fn malformed_pages_stop_advice_without_changing_sqlite_verdict() {
        let (_dir, path, connection) = fixture();
        let root: u32 = connection
            .query_row(
                "SELECT rootpage FROM sqlite_schema WHERE name = 'payload'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let count: u32 = connection
            .pragma_query_value(None, "page_count", |row| row.get(0))
            .unwrap();
        drop(connection);
        for (case, offset, replacement, expected) in [
            ("cell", 12_u64, u16::MAX.to_be_bytes().to_vec(), "cell"),
            (
                "child",
                8,
                (count + 1).to_be_bytes().to_vec(),
                "out of range",
            ),
            (
                "spine",
                8,
                root.to_be_bytes().to_vec(),
                "right spine exceeds",
            ),
        ] {
            let damaged = path.with_file_name(format!("{case}.db"));
            std::fs::copy(&path, &damaged).unwrap();
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .open(&damaged)
                .unwrap();
            file.seek(SeekFrom::Start((u64::from(root) - 1) * 8192 + offset))
                .unwrap();
            file.write_all(&replacement).unwrap();
            drop(file);
            let connection =
                Connection::open_with_flags(&damaged, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
            let setup = Setup::new(&connection, &damaged).unwrap();
            let outcome = run_worker(
                &setup,
                &AtomicBool::new(false),
                &AtomicU64::new(0),
                LEAD_BYTES / setup.page_size,
                ADVICE_CHUNK_BYTES / setup.page_size,
            );
            assert!(
                matches!(outcome, Outcome::Unavailable(ref error) if error.contains(expected)),
                "{case}"
            );
            let plain = connection
                .query_row::<String, _, _>("PRAGMA quick_check", [], |row| row.get(0))
                .map_err(|error| error.to_string());
            let (prefetched, _) = quick_check(&connection, &damaged);
            assert_eq!(
                prefetched.map_err(|error| error.to_string()),
                plain,
                "{case}"
            );
            assert_ne!(plain, Ok("ok".to_owned()), "{case}");
        }
    }

    #[test]
    fn stop_flag_cancels_worker_and_joins() {
        let (_dir, path, connection) = fixture();
        let setup = Setup::new(&connection, &path).unwrap();
        let stop = AtomicBool::new(true);
        let counter = AtomicU64::new(0);
        let outcome = thread::scope(|scope| {
            scope
                .spawn(|| run_worker(&setup, &stop, &counter, 1, 1))
                .join()
                .unwrap()
        });
        assert!(matches!(outcome, Outcome::Cancelled));
    }

    #[test]
    fn pacing_caps_lead_and_chunk() {
        let pages = LEAD_BYTES / 4096;
        let chunk = ADVICE_CHUNK_BYTES / 4096;
        let counter = AtomicU64::new(0);
        assert_eq!(
            advice_pages(0, counter.load(Ordering::Relaxed), pages, chunk),
            chunk
        );
        assert_eq!(
            advice_pages(pages - 2, counter.load(Ordering::Relaxed), pages, chunk),
            2
        );
        counter.store(5, Ordering::Relaxed);
        assert_eq!(
            advice_pages(pages - 2, counter.load(Ordering::Relaxed), pages, chunk),
            7
        );
        assert_eq!(
            advice_pages(pages + 6, counter.load(Ordering::Relaxed), pages, chunk),
            0
        );
        let mut advised = 0;
        for consumed in [0, 1, 255, 256, 257, 10_000] {
            counter.store(consumed, Ordering::Relaxed);
            loop {
                let allowed = advice_pages(advised, counter.load(Ordering::Relaxed), 17, 8);
                assert!(allowed <= 8);
                if allowed == 0 {
                    break;
                }
                advised += allowed;
                assert!(advised <= consumed + 17);
            }
        }
    }

    #[test]
    fn hand_off_occurs_at_the_progress_step_boundary() {
        let step = u64::from(PROGRESS_STEP_PAGES);
        for total in [1_u64, 255, 256, 257, 1_000_000] {
            let first = total / step * step;
            if first > 0 {
                assert!(!walk_finished(first - step, total));
            }
            assert!(walk_finished(first, total));
            for counter in total.saturating_sub(257)..=total + 1 {
                assert_eq!(walk_finished(counter, total), counter + 256 > total);
            }
        }
    }

    #[test]
    fn progress_counts_real_tree_page_boundaries() {
        for total in [1_u64, 255, 256, 257] {
            let (_dir, path, mut connection) = empty_fixture();
            connection
                .execute_batch("PRAGMA user_version = 1;")
                .unwrap();
            if total > 1 {
                connection
                    .execute_batch("CREATE TABLE pages (data BLOB NOT NULL);")
                    .unwrap();
                let transaction = connection.transaction().unwrap();
                for _ in 0..total - 2 {
                    transaction
                        .execute("INSERT INTO pages VALUES (zeroblob(7000))", [])
                        .unwrap();
                }
                transaction.commit().unwrap();
            }
            let actual: u64 = connection
                .query_row(
                    "SELECT count(*) FROM dbstat WHERE pagetype != 'overflow'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(actual, total);
            let setup = Setup::new(&connection, &path).unwrap();
            let stop = AtomicBool::new(false);
            let mut walked = 0;
            walk(&setup, &stop, |_| {
                walked += 1;
                Ok(())
            })
            .unwrap();
            assert_eq!(walked, actual);
            let (verdict, counter) = sqlite_progress(&connection);
            assert_eq!(verdict, "ok");
            assert!(walk_finished(counter.load(Ordering::Relaxed), walked));
            assert!(matches!(
                run_worker(
                    &setup,
                    &stop,
                    &counter,
                    LEAD_BYTES / setup.page_size,
                    ADVICE_CHUNK_BYTES / setup.page_size,
                ),
                Outcome::Done
            ));
            let (verdict, outcome) = quick_check(&connection, &path);
            assert_eq!(verdict.unwrap(), "ok");
            assert!(matches!(outcome, Outcome::Done | Outcome::Cancelled));
        }
    }

    #[test]
    fn scan_roots_and_leaf_order_match_schema_explain_and_dbstat() {
        let (_dir, path, connection) = fixture();
        connection
            .execute_batch(
                "CREATE TABLE without_rowid (key TEXT PRIMARY KEY, value TEXT) WITHOUT ROWID;
                 INSERT INTO without_rowid SELECT key, data FROM payload;",
            )
            .unwrap();
        let setup = Setup::new(&connection, &path).unwrap();
        let expected = connection
            .prepare("SELECT 1 UNION SELECT rootpage FROM sqlite_schema WHERE type = 'table'")
            .unwrap()
            .query_map([], |row| row.get::<_, u32>(0))
            .unwrap()
            .collect::<rusqlite::Result<BTreeSet<_>>>()
            .unwrap();
        assert_eq!(
            setup.scan_roots.iter().copied().collect::<BTreeSet<_>>(),
            expected
        );
        let mut order = Vec::new();
        let mut statement = connection.prepare("EXPLAIN PRAGMA quick_check").unwrap();
        let program = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                ))
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        for (index, (opcode, cursor, _, _)) in program.iter().enumerate() {
            if opcode != "Rewind" {
                continue;
            }
            if let Some((opcode, _, root, database)) =
                program[..index].iter().rev().find(|(opcode, p1, _, _)| {
                    p1 == cursor && matches!(opcode.as_str(), "OpenRead" | "Close")
                })
                && opcode == "OpenRead"
                && *database == 0
            {
                order.push(u32::try_from(*root).unwrap());
            }
        }
        assert_eq!(setup.scan_roots, order);
        let stop = AtomicBool::new(false);
        let mut leaf_root = false;
        for root in setup.scan_roots.iter().copied() {
            let mut emitted = Vec::new();
            preorder(&setup, root, &stop, true, &mut |page| {
                emitted.push(page);
                Ok(())
            })
            .unwrap();
            let mut statement = connection
                .prepare(
                    "SELECT pageno FROM dbstat WHERE name =
                 (SELECT name FROM dbstat WHERE pageno = ?1 AND path = '/')
                 AND pagetype != 'overflow' ORDER BY path",
                )
                .unwrap();
            let expected = statement
                .query_map([root], |row| row.get::<_, u32>(0))
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap();
            leaf_root |= expected == [root];
            assert_eq!(emitted, expected, "root {root}");
        }
        assert!(leaf_root);
    }

    fn test_adviser<'a>(
        setup: &'a Setup,
        stop: &'a AtomicBool,
        counter: &'a AtomicU64,
        lead: u64,
        chunk: u64,
    ) -> Adviser<'a> {
        Adviser {
            setup,
            stop,
            counter,
            lead,
            chunk,
            advised: 0,
            scan: None,
            direction: Direction::Descending,
            run_first: None,
            run_len: 0,
        }
    }

    #[test]
    fn scan_base_is_sampled_only_after_hand_off_and_advice_stays_in_lead() {
        let (dir, path, connection) = fixture();
        let mut setup = Setup::new(&connection, &path).unwrap();
        let io_path = dir.path().join("io");
        std::fs::write(&io_path, "no syscr until hand-off\n").unwrap();
        setup.io = File::open(&io_path).unwrap();
        let counter = AtomicU64::new(0);
        let stop = AtomicBool::new(true);
        let mut adviser = test_adviser(&setup, &stop, &counter, 2, 8);
        assert!(matches!(adviser.start_scan(256), Err(WorkError::Cancelled)));
        assert!(adviser.scan.is_none());
        std::fs::write(&io_path, "syscr: 37\n").unwrap();
        stop.store(false, Ordering::Relaxed);
        counter.store(256, Ordering::Relaxed);
        adviser.advised = 257;
        adviser.start_scan(257).unwrap();
        assert_eq!(adviser.scan, Some((257, 37)));
        assert_eq!(adviser.allowance().unwrap(), 2);
        adviser.push(1).unwrap();
        adviser.push(2).unwrap();
        adviser.flush().unwrap();
        assert_eq!(adviser.advised, 259);
        assert_eq!(adviser.allowance().unwrap(), 0);
        std::fs::write(&io_path, "syscr: 40\n").unwrap();
        assert_eq!(adviser.allowance().unwrap(), 3);
    }

    #[test]
    fn scan_corruption_preserves_sqlite_verdict() {
        let (_dir, path, connection) = fixture();
        connection
            .execute_batch(
                "CREATE TABLE scan_damage (value TEXT NOT NULL, padding TEXT);
                 INSERT INTO scan_damage VALUES ('x', NULL);",
            )
            .unwrap();
        let root: u32 = connection
            .query_row(
                "SELECT rootpage FROM sqlite_schema WHERE name = 'scan_damage'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        drop(connection);
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let mut bytes = vec![0; 8192];
        let offset = (u64::from(root) - 1) * 8192;
        file.read_exact_at(&mut bytes, offset).unwrap();
        let cell = usize::from(u16::from_be_bytes(bytes[8..10].try_into().unwrap()));
        // Swap the serial types: the NOT NULL column becomes NULL and padding takes
        // its text payload. The record size and structural coverage stay unchanged.
        assert_eq!(&bytes[cell + 3..cell + 5], &[15, 0]);
        bytes[cell + 3] = 0;
        bytes[cell + 4] = 15;
        file.write_all_at(&bytes, offset).unwrap();
        drop(file);
        let connection =
            Connection::open_with_flags(&path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
        let plain: String = connection
            .query_row("PRAGMA quick_check", [], |row| row.get(0))
            .unwrap();
        assert_eq!(plain, "NULL value in scan_damage.value");
        let setup = Setup::new(&connection, &path).unwrap();
        assert!(matches!(
            run_worker(
                &setup,
                &AtomicBool::new(false),
                &AtomicU64::new(setup.page_count),
                LEAD_BYTES / setup.page_size,
                ADVICE_CHUNK_BYTES / setup.page_size,
            ),
            Outcome::Done
        ));
        // SQLite generates the NOT NULL validation in the row scan after IntegrityCk.
        let mut statement = connection.prepare("EXPLAIN PRAGMA quick_check").unwrap();
        let ops = statement
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        let walk = ops
            .iter()
            .position(|opcode| opcode == "IntegrityCk")
            .unwrap();
        let scan = ops.iter().position(|opcode| opcode == "IsType").unwrap();
        assert!(walk < scan);
        let (prefetched, _) = quick_check(&connection, &path);
        assert_eq!(prefetched.unwrap(), plain);
    }

    #[test]
    fn pacing_wait_cancels_and_interrupt_removes_handler_and_joins() {
        let (_dir, path, connection) = fixture();
        let setup = Setup::new(&connection, &path).unwrap();
        let stop = AtomicBool::new(false);
        let counter = AtomicU64::new(0);
        let mut adviser = test_adviser(&setup, &stop, &counter, 0, 1);
        adviser.push(1).unwrap();
        assert_eq!(adviser.allowance().unwrap(), 0);
        thread::scope(|scope| {
            let (started, ready) = std::sync::mpsc::sync_channel(0);
            let (finished, result) = std::sync::mpsc::sync_channel(1);
            let worker = scope.spawn(move || {
                started.send(()).unwrap();
                finished.send(adviser.flush()).unwrap();
            });
            ready.recv().unwrap();
            assert!(matches!(
                result.recv_timeout(Duration::from_millis(20)),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout)
            ));
            stop.store(true, Ordering::Relaxed);
            assert!(matches!(result.recv().unwrap(), Err(WorkError::Cancelled)));
            worker.join().unwrap();
        });
        connection
            .execute_batch(
                "CREATE TABLE interrupt_rows (value INTEGER NOT NULL);
                 WITH RECURSIVE rows(n) AS (VALUES(1) UNION ALL SELECT n + 1 FROM rows WHERE n < 1000000)
                 INSERT INTO interrupt_rows SELECT n FROM rows;",
            )
            .unwrap();
        let counter = Arc::new(AtomicU64::new(0));
        let interrupt = connection.get_interrupt_handle();
        let finished = AtomicBool::new(false);
        let (result, outcome) = thread::scope(|scope| {
            let interrupter = scope.spawn(|| {
                while counter.load(Ordering::Relaxed) == 0 && !finished.load(Ordering::Relaxed) {
                    thread::yield_now();
                }
                interrupt.interrupt();
            });
            let result = quick_check_inner(&connection, &path, Some((0, 1)), Arc::clone(&counter));
            finished.store(true, Ordering::Relaxed);
            interrupter.join().unwrap();
            result
        });
        // An interrupt that lands after SQLite finished leaves its verdict; either way the
        // result is SQLite's own, the handler is gone and the worker has been joined.
        assert!(
            matches!(&result, Err(rusqlite::Error::SqliteFailure(error, _))
                if error.code == rusqlite::ErrorCode::OperationInterrupted)
                || matches!(&result, Ok(verdict) if verdict == "ok"),
            "{result:?}"
        );
        assert!(matches!(outcome, Outcome::Cancelled | Outcome::Done));
        assert_eq!(Arc::strong_count(&counter), 1);
        let before = counter.load(Ordering::Relaxed);
        let plain: String = connection
            .query_row("PRAGMA quick_check", [], |row| row.get(0))
            .unwrap();
        assert_eq!(plain, "ok");
        assert_eq!(counter.load(Ordering::Relaxed), before);
    }

    #[test]
    fn setup_failure_keeps_the_plain_pragma_without_a_handler() {
        let (_dir, path, connection) = fixture();
        let counter = Arc::new(AtomicU64::new(0));
        let (verdict, outcome) = quick_check_inner(
            &connection,
            &path.with_file_name("absent.db"),
            None,
            Arc::clone(&counter),
        );
        assert_eq!(verdict.unwrap(), "ok");
        assert!(matches!(outcome, Outcome::Unavailable(_)));
        assert_eq!(counter.load(Ordering::Relaxed), 0);
        assert_eq!(Arc::strong_count(&counter), 1);
    }
}
