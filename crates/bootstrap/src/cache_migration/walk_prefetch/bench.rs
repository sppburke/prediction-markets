//! Cold-cache `quick_check` wall time (including the helper join) and I/O for both
//! SQLite passes. Samples contain cumulative checking-thread rchar/syscr/read_bytes
//! and process read_bytes; subtract the first sample to exclude fixture construction.
//!
//! Build with `cargo test -p pe-bootstrap --no-run`, then run the printed library
//! test binary directly under `systemd-run --user --scope -p MemoryMax=2G` with
//! `--ignored --exact cache_migration::walk_prefetch::bench::quick_check_benchmark --nocapture`.
//! Set PE_QUICK_CHECK_BENCH_DB and PE_QUICK_CHECK_BENCH_OUT to the fixture and
//! append-only JSON-lines paths. The fixture is built if absent; its SHA-256 is
//! written once to <db>.sha256 and computed during a measured run only on request:
//! PE_QUICK_CHECK_BENCH_HASH=hash runs the production candidate hash (`sha256_file`,
//! verified against <db>.sha256) between the cold reset and the check, as activation
//! does, and `hash-release` then drops the file's cached pages as normal activation
//! does (`release_cached_pages`, #790). The result line records hash_ms, release_ms
//! and total_ms, from the hash start (or the cold reset) through check completion.
//! PE_QUICK_CHECK_BENCH_MIB defaults to 8192, PE_QUICK_CHECK_BENCH_LABEL labels every
//! line, and PE_QUICK_CHECK_BENCH_CACHE_PAGES optionally overrides cache_size (16
//! for the positional strace diagnostic). The connection is immutable/read-only,
//! with mmap_size left at its default zero.
//!
//! Each invocation advises DONTNEED over the whole fixture. The caller's run script
//! must independently confirm residency below 1% before each run. To compare code
//! changes, alternate three main runs and three branch runs on the same fixture,
//! settings and memory cap. To time the release itself (#790), alternate `hash` and
//! `hash-release` runs from one binary and compare total_ms.
//! The 2 GiB cap holds the helper's 1 GiB lead while keeping an 8 GiB fixture four
//! times larger than the available page cache. Record filesystem/read-ahead here;
//! use strace -f -tt -e trace=pread64,fadvise64 for the separate positional run.

use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::Write as _;
use std::os::unix::fs::{FileExt as _, MetadataExt as _};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result, anyhow, ensure};
use rusqlite::{Connection, OpenFlags, Statement, params};
use rustix::fs::{Advice, fadvise, fstatfs, major, minor};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};

#[test]
#[ignore = "benchmark: see the module comment"]
fn quick_check_benchmark() -> Result<()> {
    let path = PathBuf::from(
        env::var_os("PE_QUICK_CHECK_BENCH_DB").context("set PE_QUICK_CHECK_BENCH_DB")?,
    );
    let output_path = PathBuf::from(
        env::var_os("PE_QUICK_CHECK_BENCH_OUT").context("set PE_QUICK_CHECK_BENCH_OUT")?,
    );
    let label = env::var("PE_QUICK_CHECK_BENCH_LABEL").unwrap_or_default();
    if !path.exists() {
        let mib = env_integer("PE_QUICK_CHECK_BENCH_MIB")?.unwrap_or(8192);
        ensure!(mib > 0, "fixture size must be positive");
        build_fixture(&path, mib)?;
    }
    let path = path.canonicalize()?;
    let fixture_sha256 = fs::read_to_string(hash_path(&path))?.trim().to_owned();
    let file = File::open(&path)?;
    let device = file.metadata()?.dev();
    let fixture_bytes = file.metadata()?.len();
    let filesystem = format!("{:#x}", fstatfs(&file)?.f_type);
    let (read_ahead_kb, read_ahead_error) = match device_read_ahead(device) {
        Ok(value) => (Some(value), None),
        Err(error) => (None, Some(error.to_string())),
    };
    let kernel = fs::read_to_string("/proc/sys/kernel/osrelease")?
        .trim()
        .to_owned();
    let mut output = OpenOptions::new()
        .create(true)
        .append(true)
        .open(output_path)?;

    fadvise(&file, 0, None, Advice::DontNeed)?;
    let hash_mode = env::var("PE_QUICK_CHECK_BENCH_HASH").unwrap_or_default();
    ensure!(
        matches!(hash_mode.as_str(), "" | "hash" | "hash-release"),
        "PE_QUICK_CHECK_BENCH_HASH must be empty, hash or hash-release"
    );
    let sequence_started = Instant::now();
    let (hash_ms, release_ms) = if hash_mode.is_empty() {
        (None, None)
    } else {
        let hash_started = Instant::now();
        let digest = super::super::sha256_file(&path)?;
        let hash_ms = hash_started.elapsed().as_millis();
        ensure!(
            digest == fixture_sha256,
            "fixture hash differs from <db>.sha256"
        );
        let release_ms = (hash_mode == "hash-release").then(|| {
            let release_started = Instant::now();
            super::super::release_cached_pages(&path);
            release_started.elapsed().as_millis()
        });
        (Some(hash_ms), release_ms)
    };
    let uri = immutable_uri(&path)?;
    let connection = Connection::open_with_flags(
        uri,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )?;
    if let Some(pages) = env_integer("PE_QUICK_CHECK_BENCH_CACHE_PAGES")? {
        ensure!(
            pages > 0,
            "cache_size override must be a positive page count"
        );
        connection.pragma_update(None, "cache_size", i64::try_from(pages)?)?;
    }
    let cache_size: i64 = connection.pragma_query_value(None, "cache_size", |row| row.get(0))?;
    let mmap_size: i64 = connection.pragma_query_value(None, "mmap_size", |row| row.get(0))?;
    let page_size: u64 = connection.pragma_query_value(None, "page_size", |row| row.get(0))?;
    let page_count: u64 = connection.pragma_query_value(None, "page_count", |row| row.get(0))?;
    ensure!(mmap_size == 0, "default mmap_size must be zero");

    // Open on the checking thread: the sampler must not resolve thread-self itself.
    let checking_io = File::open("/proc/thread-self/io")?;
    let process_io = File::open("/proc/self/io")?;
    let (ready_tx, ready_rx) = mpsc::sync_channel(0);
    let (start_tx, start_rx) = mpsc::sync_channel(0);
    let (stop_tx, stop_rx) = mpsc::channel::<()>();
    let (verdict, outcome, wall_ms, total_ms) = thread::scope(|scope| -> Result<_> {
        let output = &mut output;
        let label = &label;
        let sampler = thread::Builder::new()
            .name("quick-check-bench-sampler".to_owned())
            .spawn_scoped(scope, move || -> Result<()> {
                let initial_checking = io_counters(&checking_io)?;
                let initial_process = io_counters(&process_io)?;
                ready_tx.send(())?;
                let started: Instant = start_rx.recv()?;
                write_sample(output, label, 0, initial_checking, initial_process)?;
                loop {
                    let stopped = match stop_rx.recv_timeout(Duration::from_secs(1)) {
                        Err(RecvTimeoutError::Timeout) => false,
                        Ok(()) | Err(RecvTimeoutError::Disconnected) => true,
                    };
                    write_sample(
                        output,
                        label,
                        started.elapsed().as_millis(),
                        io_counters(&checking_io)?,
                        io_counters(&process_io)?,
                    )?;
                    if stopped {
                        return Ok(());
                    }
                }
            })?;
        ready_rx.recv()?;
        start_tx.send(Instant::now())?;
        let started = Instant::now();
        let (verdict, outcome) = super::quick_check(&connection, &path);
        let wall_ms = started.elapsed().as_millis();
        let total_ms = sequence_started.elapsed().as_millis();
        drop(stop_tx);
        sampler
            .join()
            .map_err(|_| anyhow!("benchmark sampler panicked"))??;
        Ok((verdict, outcome, wall_ms, total_ms))
    })?;
    let (verdict_text, error) = match &verdict {
        Ok(value) => (Some(value.as_str()), None),
        Err(error) => (None, Some(error.to_string())),
    };
    write_line(
        &mut output,
        &json!({
            "kind": "result", "label": label,
            "verdict": verdict_text, "error": error, "prefetch": outcome.status(),
            "wall_ms": wall_ms, "hash_mode": hash_mode, "hash_ms": hash_ms,
            "release_ms": release_ms, "total_ms": total_ms,
            "cache_size": cache_size, "mmap_size": mmap_size,
            "page_size": page_size, "page_count": page_count,
            "sqlite_version": rusqlite::version(), "kernel": kernel,
            "filesystem": filesystem, "read_ahead_kb": read_ahead_kb,
            "read_ahead_error": read_ahead_error,
            "fixture_sha256": fixture_sha256, "fixture_bytes": fixture_bytes,
            "fixture_path": path,
        }),
    )?;
    ensure!(verdict? == "ok", "fixture failed quick_check");
    Ok(())
}

fn env_integer(name: &str) -> Result<Option<u64>> {
    env::var_os(name)
        .map(|value| {
            value
                .to_str()
                .context("benchmark integer is not UTF-8")?
                .parse()
                .with_context(|| format!("invalid {name}"))
        })
        .transpose()
}

fn immutable_uri(path: &Path) -> Result<String> {
    let escaped = path
        .to_str()
        .context("fixture path is not UTF-8")?
        .replace('%', "%25")
        .replace('?', "%3F")
        .replace('#', "%23");
    Ok(format!("file:{escaped}?immutable=1"))
}

fn device_read_ahead(device: u64) -> Result<u64> {
    let device_path = PathBuf::from(format!(
        "/sys/dev/block/{}:{}",
        major(device),
        minor(device)
    ))
    .canonicalize()?;
    let queue = if device_path.join("partition").exists() {
        device_path
            .parent()
            .context("partition has no parent device")?
            .join("queue")
    } else {
        device_path.join("queue")
    };
    let path = queue.join("read_ahead_kb");
    fs::read_to_string(&path)?
        .trim()
        .parse()
        .with_context(|| format!("invalid {}", path.display()))
}

#[derive(Clone, Copy)]
struct IoCounters {
    rchar: u64,
    syscr: u64,
    read_bytes: u64,
}

fn io_counters(file: &File) -> Result<IoCounters> {
    let mut bytes = [0_u8; 4096];
    let len = file.read_at(&mut bytes, 0)?;
    let text = std::str::from_utf8(&bytes[..len])?;
    let value = |key: &str| -> Result<u64> {
        text.lines()
            .find_map(|line| line.strip_prefix(key))
            .with_context(|| format!("missing {key} in proc io"))?
            .trim()
            .parse()
            .with_context(|| format!("invalid {key} in proc io"))
    };
    Ok(IoCounters {
        rchar: value("rchar:")?,
        syscr: value("syscr:")?,
        read_bytes: value("read_bytes:")?,
    })
}

fn write_sample(
    output: &mut File,
    label: &str,
    elapsed_ms: u128,
    checking: IoCounters,
    process: IoCounters,
) -> Result<()> {
    write_line(
        output,
        &json!({
            "kind": "sample", "label": label, "elapsed_ms": elapsed_ms,
            "rchar": checking.rchar, "syscr": checking.syscr, "read_bytes": checking.read_bytes,
            "process_read_bytes": process.read_bytes,
        }),
    )
}

fn write_line(output: &mut File, value: &Value) -> Result<()> {
    serde_json::to_writer(&mut *output, value)?;
    output.write_all(b"\n")?;
    output.flush()?;
    Ok(())
}

const OVERFLOW_ROW_EVERY: u64 = 10_000;

struct Xorshift(u64);

impl Xorshift {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

fn hash_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(".sha256");
    PathBuf::from(name)
}

fn build_fixture(path: &Path, mib: u64) -> Result<()> {
    let bytes = mib
        .checked_mul(1 << 20)
        .context("fixture size overflows u64")?;
    // Reference builder's 55% groups / 25% sealed budgets; remaining space is indexes.
    // One activity row in 10,000 carries an overflow body: Forge's large tables sampled
    // about none (#694), so chains stay present without dominating the walk.
    let group_row_bytes = 330 + (16 * 1024 - 200) / OVERFLOW_ROW_EVERY;
    let group_rows = bytes / 100 * 55 / group_row_bytes;
    let wallet_count = usize::try_from((group_rows / 2).clamp(9, 40_000))?;
    let rows_per_wallet = group_rows / u64::try_from(wallet_count)?;
    let sealed_rows = bytes / 100 * 25 / 110;
    let mut rng = Xorshift(694);
    let mut wallets = (0..wallet_count)
        .map(|_| {
            format!(
                "0x{:016x}{:016x}{:08x}",
                rng.next(),
                rng.next(),
                rng.next() >> 32
            )
        })
        .collect::<Vec<_>>();
    wallets.sort();
    let mut connection = Connection::open(path)?;
    connection.execute_batch(
        "PRAGMA page_size=4096;
         PRAGMA auto_vacuum=INCREMENTAL;
         PRAGMA journal_mode=WAL;
         PRAGMA synchronous=OFF;
         PRAGMA cache_size=-262144;
         CREATE TABLE groups (
             id TEXT NOT NULL, wallet TEXT NOT NULL, t INTEGER NOT NULL,
             kind TEXT NOT NULL, amount TEXT NOT NULL, body TEXT NOT NULL, v INTEGER NOT NULL
         );
         CREATE UNIQUE INDEX groups_id ON groups(id);
         CREATE INDEX groups_wallet_time ON groups(wallet, t, id);
         CREATE TABLE sealed (
             market TEXT, wallet TEXT, side TEXT, price INTEGER, size TEXT, ts INTEGER, v INTEGER
         );
         CREATE TABLE small (id TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID;
         INSERT INTO small VALUES ('fixture', 'quick_check');",
    )?;
    let mut next_rowid = 1_u64;
    for batch in wallets.chunks(500) {
        let transaction = connection.transaction()?;
        {
            let mut insert = transaction.prepare(
                "INSERT INTO groups (rowid, id, wallet, t, kind, amount, body, v)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            )?;
            for wallet in batch {
                insert_wallet(
                    &mut insert,
                    wallet,
                    rows_per_wallet,
                    1_700_000_000,
                    &mut next_rowid,
                    &mut rng,
                )?;
            }
        }
        transaction.commit()?;
    }
    for start in (0..sealed_rows).step_by(100_000) {
        let transaction = connection.transaction()?;
        {
            let mut insert =
                transaction.prepare("INSERT INTO sealed VALUES (?, ?, ?, ?, ?, ?, ?)")?;
            for i in start..(start + 100_000).min(sealed_rows) {
                let market = format!("m{:08x}", rng.next() >> 32);
                let wallet = &wallets[usize::try_from(rng.next() % u64::try_from(wallet_count)?)?];
                let price = i64::try_from(rng.next() % 100)?;
                insert.execute(params![
                    market,
                    wallet,
                    "buy",
                    price,
                    "12.5",
                    1_700_000_000 + i64::try_from(i)?,
                    1,
                ])?;
            }
        }
        transaction.commit()?;
    }
    let mut indices = (0..wallet_count).collect::<Vec<_>>();
    for _ in 0..3 {
        // Partial Fisher-Yates: a ninth of the wallets, without replacement per round.
        for i in 0..wallet_count / 9 {
            let chosen = i + usize::try_from(rng.next() % u64::try_from(wallet_count - i)?)?;
            indices.swap(i, chosen);
        }
        let transaction = connection.transaction()?;
        {
            let mut delete = transaction.prepare("DELETE FROM groups WHERE wallet = ?")?;
            let mut insert = transaction.prepare(
                "INSERT INTO groups (rowid, id, wallet, t, kind, amount, body, v)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            )?;
            for index in &indices[..wallet_count / 9] {
                let wallet = &wallets[*index];
                delete.execute([wallet])?;
                insert_wallet(
                    &mut insert,
                    wallet,
                    rows_per_wallet,
                    1_800_000_000,
                    &mut next_rowid,
                    &mut rng,
                )?;
            }
        }
        transaction.commit()?;
    }
    connection.execute_batch("PRAGMA wal_checkpoint(TRUNCATE); PRAGMA journal_mode=DELETE;")?;
    connection.close().map_err(|(_, error)| error)?;
    let mut file = File::open(path)?;
    file.sync_all()?;
    let mut digest = Sha256::new();
    std::io::copy(&mut file, &mut digest)?;
    fs::write(hash_path(path), format!("{:x}\n", digest.finalize()))?;
    Ok(())
}

fn insert_wallet(
    insert: &mut Statement<'_>,
    wallet: &str,
    count: u64,
    start: i64,
    next_rowid: &mut u64,
    rng: &mut Xorshift,
) -> Result<()> {
    let normal_body = "b".repeat(200);
    for i in 0..count {
        let id = format!("g2:{:016x}{:08x}", rng.next(), rng.next() >> 32);
        let amount = format!("0.{:06}", rng.next() % 1_000_000);
        let body = if rng.next().is_multiple_of(OVERFLOW_ROW_EVERY) {
            "b".repeat(usize::try_from(12 * 1024 + rng.next() % (8 * 1024 + 1))?)
        } else {
            normal_body.clone()
        };
        insert.execute(params![
            i64::try_from(*next_rowid)?,
            id,
            wallet,
            start + i64::try_from(i)? * 37,
            "trade",
            amount,
            body,
            2,
        ])?;
        *next_rowid += 1;
    }
    Ok(())
}
