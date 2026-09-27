use std::fs::File;
use std::num::NonZeroU64;
use std::os::unix::fs::FileExt as _;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use rusqlite::Connection;
use rustix::fs::{Advice, fadvise};

// Canonical defaults: docs/_GLOSSARY.md.
const LEAD_BYTES: u64 = 1 << 30;
const ADVICE_CHUNK_BYTES: u64 = 4 << 20;
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
    page_size: u64,
    page_count: u64,
    start_reads: u64,
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
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, i64>(6)?,
                ))
            })
            .map_err(|error| format!("explain quick_check rows: {error}"))?;
        let mut roots = None;
        for row in rows {
            let (opcode, p4, p5) = row.map_err(|error| format!("explain row: {error}"))?;
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
                break;
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
        let start_reads = read_syscr(&io)?;
        Ok(Self {
            file,
            io,
            roots,
            page_size,
            page_count: bytes / page_size,
            start_reads,
        })
    }
}

struct StopOnDrop<'a>(&'a AtomicBool);

impl Drop for StopOnDrop<'_> {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

pub(super) fn quick_check(
    connection: &Connection,
    path: &Path,
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
    let stop = AtomicBool::new(false);
    thread::scope(|scope| {
        let worker = thread::Builder::new()
            .name("quick-check-prefetch".to_owned())
            .spawn_scoped(scope, || run_worker(&setup, &stop));
        let result = {
            let _guard = StopOnDrop(&stop);
            connection.query_row("PRAGMA quick_check", [], |row| row.get(0))
        };
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

// syscr includes SQLite's non-tree reads. It is a pacing heuristic, not a cache bound.
fn advice_pages(advised: u64, syscr: u64, start: u64, page_size: u64) -> u64 {
    let lead = LEAD_BYTES / page_size;
    let chunk = ADVICE_CHUNK_BYTES / page_size;
    syscr
        .saturating_sub(start)
        .saturating_add(lead)
        .saturating_sub(advised)
        .min(chunk)
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

fn run_worker(setup: &Setup, stop: &AtomicBool) -> Outcome {
    let mut adviser = Adviser {
        setup,
        stop,
        advised: 0,
        sampled_reads: setup.start_reads,
        run_high: None,
        run_len: 0,
    };
    let result = walk(setup, stop, |page| adviser.push(page)).and_then(|()| adviser.flush());
    match result {
        Ok(()) => Outcome::Done,
        Err(WorkError::Cancelled) => Outcome::Cancelled,
        Err(WorkError::Unavailable(error)) => Outcome::Unavailable(error),
    }
}

struct Adviser<'a> {
    setup: &'a Setup,
    stop: &'a AtomicBool,
    advised: u64,
    sampled_reads: u64,
    run_high: Option<u32>,
    run_len: u64,
}

impl Adviser<'_> {
    fn push(&mut self, page: u32) -> WorkResult<()> {
        check_stop(self.stop)?;
        if let Some(high) = self.run_high {
            let expected = u64::from(high).saturating_sub(self.run_len);
            if u64::from(page) == expected
                && self.run_len < ADVICE_CHUNK_BYTES / self.setup.page_size
            {
                self.run_len += 1;
                return Ok(());
            }
            self.flush()?;
        }
        self.run_high = Some(page);
        self.run_len = 1;
        Ok(())
    }

    fn flush(&mut self) -> WorkResult<()> {
        let Some(high) = self.run_high.take() else {
            return Ok(());
        };
        let mut remaining = self.run_len;
        let mut high = u64::from(high);
        self.run_len = 0;
        while remaining > 0 {
            check_stop(self.stop)?;
            let mut allowed = advice_pages(
                self.advised,
                self.sampled_reads,
                self.setup.start_reads,
                self.setup.page_size,
            );
            if allowed == 0 {
                loop {
                    check_stop(self.stop)?;
                    self.sampled_reads =
                        read_syscr(&self.setup.io).map_err(WorkError::Unavailable)?;
                    allowed = advice_pages(
                        self.advised,
                        self.sampled_reads,
                        self.setup.start_reads,
                        self.setup.page_size,
                    );
                    if allowed >= ADVICE_CHUNK_BYTES / self.setup.page_size {
                        break;
                    }
                    thread::sleep(Duration::from_millis(1));
                }
            }
            let take = remaining.min(allowed);
            let offset = (high - take) * self.setup.page_size;
            let bytes = take * self.setup.page_size;
            let len = NonZeroU64::new(bytes)
                .ok_or_else(|| WorkError::Unavailable("zero advice length".to_owned()))?;
            check_stop(self.stop)?;
            fadvise(&self.setup.file, offset, Some(len), Advice::WillNeed)
                .map_err(|error| WorkError::Unavailable(format!("fadvise: {error}")))?;
            self.advised += take;
            remaining -= take;
            high -= take;
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
        if root == 0 {
            continue;
        }
        let mut spine = root;
        let mut depth = None;
        for level in 0..SPINE_LIMIT {
            let bytes = page(setup, spine, stop)?;
            let page_kind = kind(&bytes, spine)?;
            if matches!(page_kind, 0x0a | 0x0d) {
                depth = Some(level);
                break;
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
        let depth = depth
            .ok_or_else(|| WorkError::Unavailable("right spine exceeds 20 levels".to_owned()))?;
        let mut stack = vec![(root, 0)];
        while let Some((number, level)) = stack.pop() {
            check_stop(stop)?;
            if level > 0 {
                emit(number)?;
            }
            if level == depth {
                continue;
            }
            let bytes = page(setup, number, stop)?;
            for child in children(&bytes, number, setup.page_count)? {
                stack.push((child, level + 1));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::cmp::Ordering as CmpOrdering;
    use std::io::{Seek as _, SeekFrom, Write as _};

    use rusqlite::{OpenFlags, params};

    use super::*;

    fn fixture() -> (tempfile::TempDir, std::path::PathBuf, Connection) {
        let target = std::env::var_os("CARGO_TARGET_DIR").map_or_else(
            || std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target"),
            std::path::PathBuf::from,
        );
        std::fs::create_dir_all(&target).unwrap();
        let dir = tempfile::tempdir_in(target).unwrap();
        let path = dir.path().join("walk.db");
        let mut connection = Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "PRAGMA page_size=8192; PRAGMA auto_vacuum=INCREMENTAL;
            CREATE TABLE payload (id INTEGER PRIMARY KEY, key TEXT NOT NULL, data TEXT NOT NULL);
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
        for number in 0..200 {
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
            let mut rows = connection.prepare(
                "SELECT path, pageno FROM dbstat WHERE name = ?1 AND pagetype != 'overflow' AND path != '/'",
            ).unwrap();
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
        assert!(matches!(run_worker(&setup, &stop), Outcome::Done));
        let plain: String = connection
            .query_row("PRAGMA quick_check", [], |row| row.get(0))
            .unwrap();
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
            let outcome = run_worker(&setup, &AtomicBool::new(false));
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
        let outcome =
            thread::scope(|scope| scope.spawn(|| run_worker(&setup, &stop)).join().unwrap());
        assert!(matches!(outcome, Outcome::Cancelled));
    }

    #[test]
    fn pacing_caps_lead_and_chunk() {
        let pages = LEAD_BYTES / 4096;
        assert_eq!(advice_pages(0, 9, 9, 4096), ADVICE_CHUNK_BYTES / 4096);
        assert_eq!(advice_pages(pages - 2, 9, 9, 4096), 2);
        assert_eq!(advice_pages(pages - 2, 14, 9, 4096), 7);
        assert_eq!(advice_pages(pages + 6, 14, 9, 4096), 0);
        assert_eq!(advice_pages(0, 9, 9, 8192), ADVICE_CHUNK_BYTES / 8192);
    }
}
