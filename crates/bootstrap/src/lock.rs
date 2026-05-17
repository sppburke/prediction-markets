//! Cross-process mutex for `pe-bootstrap` cache mutations (issue #191 Item 2).
//!
//! The wallet cache holds two pieces of cursor state that any concurrent
//! mutator can race on with lost-update semantics:
//!
//! 1. [`crate::migrate::CURSOR_WALLET_ENUM_TOPIC_HASHES`] — the per-topic
//!    enumeration progress list.
//! 2. [`crate::migrate::CURSOR_WALLET_ENUM_CHUNK_PROGRESS`] — the per-`(topic,
//!    contract)` chunk-level resume cursor.
//!
//! A normal `pe-bootstrap` sweep loads these into memory at the top of the
//! OnChain enumeration arm and writes them back over many minutes/hours of
//! `eth_getLogs` work. The `pe-bootstrap --backfill-v1-attribution`
//! subcommand introduced in #191 also reads-modifies-writes the same cursors.
//! Without coordination, a subcommand-clear can be silently undone by the
//! in-flight sweep's eventual write, leaving the V1-attribution backfill
//! invisible.
//!
//! [`CacheMutationLock`] is a PID-based exclusive lock file colocated with
//! the cache (`<cache_path>.lock`). Acquisition uses
//! `OpenOptions::create_new` which is atomic at the filesystem level —
//! exactly one process wins on a race. Stale-PID reclaim handles the case
//! where a previous holder crashed without dropping the lock.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;

use crate::error::BootstrapError;

/// Exclusive cross-process lock guarding cursor mutations against the wallet
/// cache. Held via RAII: the lock file is removed on `Drop`.
///
/// **Acquisition semantics**:
/// - Atomic `create_new` on the lock path — at most one process holds the
///   lock at any time. If the file already exists, attempts to read the
///   recorded PID and probe its liveness via `kill -0`. A dead-PID lock is
///   considered stale and atomically reclaimed.
/// - Returns [`BootstrapError::Invalid`] if a live process holds the lock,
///   naming the holder PID so the operator can investigate (typically
///   "you forgot to stop your running pe-bootstrap before running the
///   backfill subcommand").
///
/// **Drop semantics**:
/// - `Drop` removes the lock file. If removal fails (filesystem error,
///   already deleted), the error is silently swallowed — there's nothing
///   useful to propagate from `Drop` in this case.
/// - Process crash (kernel kill, panic) leaves the lock file behind. The
///   next acquisition's stale-PID reclaim handles it.
#[derive(Debug)]
#[must_use = "the lock is released as soon as the guard is dropped"]
pub struct CacheMutationLock {
    lock_path: PathBuf,
}

impl CacheMutationLock {
    /// Path component appended to `cache_path` to produce the lock filename.
    /// Lives next to the cache file so a single lock guards a single cache
    /// even when operators run multiple caches concurrently.
    const LOCK_FILE_SUFFIX: &'static str = ".lock";

    /// Attempt to acquire the lock for the given cache path. Returns the
    /// RAII guard on success.
    ///
    /// # Errors
    ///
    /// - [`BootstrapError::Invalid`] if a live PID currently holds the lock,
    ///   or if the cache path has no parent directory (`/` or relative-no-parent
    ///   case — caller misconfiguration).
    /// - [`BootstrapError::Io`] for filesystem errors (permission denied on
    ///   the parent directory, disk full, etc).
    pub fn acquire(cache_path: &Path) -> Result<Self, BootstrapError> {
        let lock_path = lock_path_for(cache_path);
        Self::acquire_at(lock_path)
    }

    /// Internal acquisition impl. Separated from [`Self::acquire`] so the
    /// stale-PID reclaim path can recurse without re-computing the path.
    fn acquire_at(lock_path: PathBuf) -> Result<Self, BootstrapError> {
        match OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&lock_path)
        {
            Ok(mut f) => {
                // Record our PID for stale-detection on the next acquire.
                writeln!(f, "{}", std::process::id())?;
                Ok(Self { lock_path })
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                Self::handle_existing_lock(lock_path)
            }
            Err(e) => Err(BootstrapError::Io(e)),
        }
    }

    /// Stale-PID reclaim path. Reads the existing lock file's PID, probes
    /// liveness, removes + retries on dead-PID, rejects on live-PID.
    fn handle_existing_lock(lock_path: PathBuf) -> Result<Self, BootstrapError> {
        let recorded_pid = std::fs::read_to_string(&lock_path)
            .ok()
            .and_then(|s| s.trim().parse::<i32>().ok());

        if let Some(pid) = recorded_pid
            && pid_is_alive(pid)
        {
            return Err(BootstrapError::Invalid {
                message: format!(
                    "cache mutation lock {} held by PID {} (still alive); refusing to mutate \
                     cursor state. Stop the running pe-bootstrap (or wait for it to finish) \
                     and retry.",
                    lock_path.display(),
                    pid
                ),
            });
        }

        // Stale lock (no PID parseable, OR PID is dead). Remove and retry.
        // The retry's `create_new` is atomic — concurrent reclaim attempts
        // serialize via the filesystem.
        std::fs::remove_file(&lock_path).or_else(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                Ok(())
            } else {
                Err(e)
            }
        })?;
        Self::acquire_at(lock_path)
    }

    /// Borrow the lock-file path for tests / diagnostics.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.lock_path
    }
}

impl Drop for CacheMutationLock {
    fn drop(&mut self) {
        // Best-effort removal. If the file was already removed (e.g. by
        // another process's stale-reclaim path), we don't care. Other
        // I/O errors here are unrecoverable and not actionable from Drop.
        let _ = std::fs::remove_file(&self.lock_path);
    }
}

/// Compute the lock file path for a given cache path. Exposed for tests so
/// they can assert the lock is at the expected location without acquiring.
#[must_use]
pub fn lock_path_for(cache_path: &Path) -> PathBuf {
    let mut path = cache_path.as_os_str().to_owned();
    path.push(CacheMutationLock::LOCK_FILE_SUFFIX);
    PathBuf::from(path)
}

/// Probe whether a PID is alive using `kill -0` (POSIX no-op signal that
/// errors out on dead/non-existent processes). Returns `false` on any
/// error (subprocess spawn failure, kill returning non-zero, etc) so the
/// caller errs toward reclaim rather than blocking on a stale lock.
fn pid_is_alive(pid: i32) -> bool {
    Command::new("kill")
        .args(["-0", &pid.to_string()])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn fake_cache_path(dir: &TempDir) -> PathBuf {
        dir.path().join("wallet_cache.db")
    }

    /// PASS: lock acquires successfully on a fresh path. Lock file exists
    ///       at `<cache>.lock` with the holder's PID recorded.
    /// FAIL: acquire fails for a path with a normal parent directory.
    #[test]
    fn acquire_creates_lock_file_with_pid() {
        let dir = TempDir::new().unwrap();
        let cache_path = fake_cache_path(&dir);
        let lock = CacheMutationLock::acquire(&cache_path).unwrap();
        assert!(lock.path().exists(), "lock file must exist after acquire");
        let recorded = std::fs::read_to_string(lock.path()).unwrap();
        let pid: i32 = recorded.trim().parse().unwrap();
        assert_eq!(pid, std::process::id() as i32);
    }

    /// PASS: dropping the guard removes the lock file. Re-acquire works.
    /// FAIL: the lock file persists after drop (RAII contract broken).
    #[test]
    fn drop_releases_lock_and_allows_reacquire() {
        let dir = TempDir::new().unwrap();
        let cache_path = fake_cache_path(&dir);
        let lock_path = lock_path_for(&cache_path);

        {
            let _lock = CacheMutationLock::acquire(&cache_path).unwrap();
            assert!(lock_path.exists());
        }
        assert!(
            !lock_path.exists(),
            "Drop must remove the lock file; re-acquire would fail otherwise"
        );

        // Second acquisition works because the first one was released.
        let _lock = CacheMutationLock::acquire(&cache_path).unwrap();
    }

    /// PASS: while a guard is held, a second acquire fails with `Invalid`
    ///       error naming our own PID.
    /// FAIL: second acquire succeeds — the mutex semantics are broken,
    ///       cursor mutations from concurrent processes can race.
    #[test]
    fn concurrent_acquire_returns_invalid_naming_holder_pid() {
        let dir = TempDir::new().unwrap();
        let cache_path = fake_cache_path(&dir);
        let _first = CacheMutationLock::acquire(&cache_path).unwrap();

        let result = CacheMutationLock::acquire(&cache_path);
        match result {
            Err(BootstrapError::Invalid { message }) => {
                let self_pid = std::process::id().to_string();
                assert!(
                    message.contains(&self_pid),
                    "error message must name the holder PID ({self_pid}); got: {message}"
                );
                assert!(
                    message.contains("Stop the running pe-bootstrap"),
                    "error message must guide the operator to stop the holder; got: {message}"
                );
            }
            other => panic!("expected Invalid error, got: {other:?}"),
        }
    }

    /// PASS: a lock file naming a dead PID is reclaimed; the new acquire
    ///       succeeds and overwrites the PID with our own.
    /// FAIL: dead-PID lock blocks forever, requiring operator cleanup
    ///       after every crash.
    #[test]
    fn stale_pid_lock_is_reclaimed() {
        let dir = TempDir::new().unwrap();
        let cache_path = fake_cache_path(&dir);
        let lock_path = lock_path_for(&cache_path);

        // Synthesize a stale lock by writing a PID that's vanishingly
        // unlikely to be alive. PID 999_999 is well above the default
        // kernel.pid_max (4M on modern Linux but rarely consumed past 5
        // digits in practice) — kill -0 999999 returns "no such process"
        // on a fresh test environment.
        std::fs::write(&lock_path, "999999\n").unwrap();
        assert!(lock_path.exists());

        let lock = CacheMutationLock::acquire(&cache_path).unwrap();
        let new_pid: i32 = std::fs::read_to_string(lock.path())
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        assert_eq!(
            new_pid,
            std::process::id() as i32,
            "stale-reclaim must overwrite the dead PID with the current process's"
        );
    }

    /// PASS: a malformed lock file (non-numeric content) is treated as
    ///       stale and reclaimed — `kill -0` can't probe a non-PID, so we
    ///       err toward reclaim.
    /// FAIL: garbage lock file blocks forever.
    #[test]
    fn malformed_lock_file_is_reclaimed_as_stale() {
        let dir = TempDir::new().unwrap();
        let cache_path = fake_cache_path(&dir);
        let lock_path = lock_path_for(&cache_path);

        std::fs::write(&lock_path, "this is not a pid\n").unwrap();
        let _lock = CacheMutationLock::acquire(&cache_path).unwrap();
        // Acquire succeeded — the malformed lock was reclaimed.
    }

    /// PASS: `lock_path_for` produces `<cache>.lock` regardless of cache
    ///       filename or extension. Important: tests pin the convention
    ///       so log-parsers and operator runbooks can predict the path.
    #[test]
    fn lock_path_for_appends_dot_lock_suffix() {
        assert_eq!(
            lock_path_for(Path::new("/tmp/foo.db")),
            PathBuf::from("/tmp/foo.db.lock")
        );
        assert_eq!(
            lock_path_for(Path::new("/tmp/no-extension")),
            PathBuf::from("/tmp/no-extension.lock")
        );
        assert_eq!(
            lock_path_for(Path::new("relative.db")),
            PathBuf::from("relative.db.lock")
        );
    }
}
