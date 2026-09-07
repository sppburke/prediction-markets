//! Cross-process mutex and verified activation handoff for Forge cache mutations (#544/#545).
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

/// Cutover guard holding the three persistent Forge locks in the only allowed
/// order: loop → one-shot run → cache (#544/#545).
#[derive(Debug)]
#[must_use = "all Forge activation locks release when the guard is dropped"]
pub struct ForgeActivationLocks {
    _loop_file: Option<File>,
    _run_file: Option<File>,
    _cache: CacheMutationLock,
}

/// One inherited shell lock proven to refer to the expected persistent inode.
///
/// The descriptor stays owned by the invoking shell (and inherited across
/// `exec`).  Rust validates it before skipping only that already-held lock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InheritedForgeLock {
    pub fd: u32,
    pub holder_pid: u32,
}

/// Explicit handoff from `rank_and_push.sh` to cache activation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ForgeLockHandoff {
    pub loop_lock: Option<InheritedForgeLock>,
    pub run_lock: InheritedForgeLock,
}

impl ForgeActivationLocks {
    /// Acquire every Forge mutation owner needed for fixed-path activation.
    pub fn acquire(cache_path: &Path) -> Result<Self, BootstrapError> {
        Self::acquire_with_handoff(cache_path, None)
    }

    /// Acquire the Forge lock stack, accepting only verified inherited shell
    /// ownership for the named loop/run locks. The cache lock is never handed
    /// off and is always acquired by this process.
    pub fn acquire_with_handoff(
        cache_path: &Path,
        handoff: Option<&ForgeLockHandoff>,
    ) -> Result<Self, BootstrapError> {
        let parent = cache_path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let eval_results = parent.join("eval-results");
        let loop_path = eval_results.join(".rank_and_push_loop.lock");
        let run_path = eval_results.join(".rank_and_push.lock");
        let loop_file = match handoff.and_then(|value| value.loop_lock.as_ref()) {
            Some(inherited) => {
                verify_inherited_lock(&loop_path, "ranking loop", inherited, false)?;
                None
            }
            None => Some(acquire_named_lock(&loop_path, "ranking loop")?),
        };
        let run_file = match handoff.map(|value| &value.run_lock) {
            Some(inherited) => {
                verify_inherited_lock(&run_path, "one-shot ranking run", inherited, true)?;
                None
            }
            None => Some(acquire_named_lock(&run_path, "one-shot ranking run")?),
        };
        let cache = CacheMutationLock::acquire(cache_path)?;
        Ok(Self {
            _loop_file: loop_file,
            _run_file: run_file,
            _cache: cache,
        })
    }
}

#[cfg(target_os = "linux")]
fn verify_inherited_lock(
    path: &Path,
    owner: &str,
    inherited: &InheritedForgeLock,
    require_parent: bool,
) -> Result<(), BootstrapError> {
    use std::os::unix::fs::MetadataExt as _;

    if inherited.holder_pid == 0 {
        return Err(BootstrapError::Invalid {
            message: format!("{owner} lock handoff has an invalid holder PID"),
        });
    }
    if require_parent && process_parent_pid()? != inherited.holder_pid {
        return Err(BootstrapError::Invalid {
            message: format!("{owner} lock handoff holder is not the invoking wrapper"),
        });
    }

    let descriptor_path = PathBuf::from(format!("/proc/self/fd/{}", inherited.fd));
    let descriptor =
        std::fs::metadata(&descriptor_path).map_err(|error| BootstrapError::Invalid {
            message: format!(
                "{owner} lock handoff descriptor {} is unavailable: {error}",
                inherited.fd
            ),
        })?;
    let expected = std::fs::metadata(path)?;
    if descriptor.dev() != expected.dev() || descriptor.ino() != expected.ino() {
        return Err(BootstrapError::Invalid {
            message: format!(
                "{owner} lock handoff descriptor {} does not name {}",
                inherited.fd,
                path.display()
            ),
        });
    }

    let mut holder_file = OpenOptions::new().read(true).write(true).open(path)?;
    if read_holder_pid(&mut holder_file).as_deref()
        != Some(inherited.holder_pid.to_string().as_str())
    {
        return Err(BootstrapError::Invalid {
            message: format!("{owner} lock handoff PID stamp does not match its holder"),
        });
    }
    match holder_file.try_lock_exclusive() {
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => Ok(()),
        Err(error) => Err(BootstrapError::Io(error)),
        Ok(()) => Err(BootstrapError::Invalid {
            message: format!("{owner} lock handoff inode is not kernel-locked"),
        }),
    }
}

#[cfg(target_os = "linux")]
fn process_parent_pid() -> Result<u32, BootstrapError> {
    let status = std::fs::read_to_string("/proc/self/status")?;
    status
        .lines()
        .find_map(|line| line.strip_prefix("PPid:\t"))
        .and_then(|value| value.trim().parse::<u32>().ok())
        .ok_or_else(|| BootstrapError::Invalid {
            message: "cannot verify the invoking wrapper PID".to_owned(),
        })
}

#[cfg(not(target_os = "linux"))]
fn verify_inherited_lock(
    _path: &Path,
    owner: &str,
    _inherited: &InheritedForgeLock,
    _require_parent: bool,
) -> Result<(), BootstrapError> {
    Err(BootstrapError::Invalid {
        message: format!("{owner} lock handoff is supported only on Linux Forge hosts"),
    })
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

fn acquire_named_lock(path: &Path, owner: &str) -> Result<File, BootstrapError> {
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)?;
    if let Err(error) = file.try_lock_exclusive() {
        if error.kind() != std::io::ErrorKind::WouldBlock {
            return Err(BootstrapError::Io(error));
        }
        let holder = read_holder_pid(&mut file).unwrap_or_else(|| "unknown".to_owned());
        return Err(BootstrapError::Invalid {
            message: format!(
                "{owner} lock {} held by PID {holder}; refusing cache activation",
                path.display()
            ),
        });
    }
    file.set_len(0)?;
    file.seek(SeekFrom::Start(0))?;
    writeln!(file, "{}", std::process::id())?;
    file.flush()?;
    Ok(file)
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
