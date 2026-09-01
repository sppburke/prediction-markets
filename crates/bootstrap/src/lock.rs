//! Cross-process mutex for `pe-bootstrap` cache mutations (#544).
//!
//! [`CacheMutationLock`] uses a persistent inode beside the cache
//! (`<cache_path>.lock`) and an exclusive nonblocking kernel lock. The inode is
//! never removed: process exit closes the file descriptor and releases the
//! lock, including after an ungraceful termination. Shell `flock(1)` and this
//! `fs2` guard therefore coordinate on the same inode and `flock(2)` semantics.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use fs2::FileExt;

use crate::error::BootstrapError;

/// Exclusive kernel-held lock guarding every read-write wallet-cache open.
///
/// Acquisition opens the persistent lock inode without truncating it, takes an
/// exclusive nonblocking lock, and only then replaces its contents with the
/// live holder's decimal PID. Contention never changes the inode or its PID.
#[derive(Debug)]
#[must_use = "the lock is released as soon as the guard is dropped"]
pub struct CacheMutationLock {
    lock_path: PathBuf,
    _file: File,
}

impl CacheMutationLock {
    /// Path component appended to `cache_path` to produce the lock filename.
    const LOCK_FILE_SUFFIX: &'static str = ".lock";

    /// Attempt to acquire the persistent kernel lock for `cache_path` (#544).
    ///
    /// # Errors
    ///
    /// - [`BootstrapError::Invalid`] if another process holds the lock.
    /// - [`BootstrapError::Io`] if the lock inode cannot be opened, locked, or
    ///   stamped with the live holder PID.
    pub fn acquire(cache_path: &Path) -> Result<Self, BootstrapError> {
        let lock_path = lock_path_for(cache_path);
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)?;

        if let Err(error) = file.try_lock_exclusive() {
            if error.kind() != std::io::ErrorKind::WouldBlock {
                return Err(BootstrapError::Io(error));
            }
            let holder = read_holder_pid(&mut file).unwrap_or_else(|| "unknown".to_owned());
            return Err(BootstrapError::Invalid {
                message: format!(
                    "cache mutation lock {} held by PID {holder}; refusing to open the cache \
                     read-write. Stop the running pe-bootstrap (or wait for it to finish) and \
                     retry.",
                    lock_path.display()
                ),
            });
        }

        file.set_len(0)?;
        file.seek(SeekFrom::Start(0))?;
        writeln!(file, "{}", std::process::id())?;
        file.flush()?;

        Ok(Self {
            lock_path,
            _file: file,
        })
    }

    /// Borrow the persistent lock-file path for tests and diagnostics.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.lock_path
    }
}

/// Compute the persistent lock-file path for `cache_path`.
#[must_use]
pub fn lock_path_for(cache_path: &Path) -> PathBuf {
    let mut path = cache_path.as_os_str().to_owned();
    path.push(CacheMutationLock::LOCK_FILE_SUFFIX);
    PathBuf::from(path)
}

fn read_holder_pid(file: &mut File) -> Option<String> {
    file.seek(SeekFrom::Start(0)).ok()?;
    let mut contents = String::new();
    file.read_to_string(&mut contents).ok()?;
    let holder = contents.trim();
    if holder.is_empty() || !holder.bytes().all(|byte| byte.is_ascii_digit()) {
        None
    } else {
        Some(holder.to_owned())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn fake_cache_path(dir: &TempDir) -> PathBuf {
        dir.path().join("wallet_cache.db")
    }

    #[test]
    fn acquire_stamps_live_pid_after_locking() {
        let dir = TempDir::new().unwrap();
        let cache_path = fake_cache_path(&dir);
        let lock = CacheMutationLock::acquire(&cache_path).unwrap();
        let recorded = std::fs::read_to_string(lock.path()).unwrap();
        assert_eq!(recorded, format!("{}\n", std::process::id()));
    }

    #[test]
    fn drop_releases_kernel_lock_but_preserves_inode() {
        let dir = TempDir::new().unwrap();
        let cache_path = fake_cache_path(&dir);
        let lock_path = lock_path_for(&cache_path);

        {
            let _lock = CacheMutationLock::acquire(&cache_path).unwrap();
        }
        assert!(lock_path.is_file(), "the persistent lock inode was removed");
        let _next = CacheMutationLock::acquire(&cache_path).unwrap();
    }

    #[test]
    fn contention_preserves_live_holder_pid() {
        let dir = TempDir::new().unwrap();
        let cache_path = fake_cache_path(&dir);
        let first = CacheMutationLock::acquire(&cache_path).unwrap();
        let before = std::fs::read_to_string(first.path()).unwrap();

        let result = CacheMutationLock::acquire(&cache_path);
        match result {
            Err(BootstrapError::Invalid { message }) => {
                assert!(message.contains(&std::process::id().to_string()));
                assert!(message.contains("refusing to open the cache read-write"));
            }
            other => panic!("expected Invalid error, got: {other:?}"),
        }
        assert_eq!(std::fs::read_to_string(first.path()).unwrap(), before);
    }

    #[test]
    fn stale_pid_text_never_blocks_an_unlocked_inode() {
        let dir = TempDir::new().unwrap();
        let cache_path = fake_cache_path(&dir);
        let lock_path = lock_path_for(&cache_path);
        std::fs::write(&lock_path, "999999\n").unwrap();

        let lock = CacheMutationLock::acquire(&cache_path).unwrap();
        assert_eq!(
            std::fs::read_to_string(lock.path()).unwrap(),
            format!("{}\n", std::process::id())
        );
    }

    #[test]
    fn lock_path_for_appends_dot_lock_suffix() {
        assert_eq!(
            lock_path_for(Path::new("/tmp/foo.db")),
            PathBuf::from("/tmp/foo.db.lock")
        );
        assert_eq!(
            lock_path_for(Path::new("relative.db")),
            PathBuf::from("relative.db.lock")
        );
    }
}
