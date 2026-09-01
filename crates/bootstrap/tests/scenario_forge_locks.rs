//! Real-process Forge lock interoperability and legacy-transition scenarios (#544).

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use fs2::FileExt;
use pe_bootstrap::lock::{CacheMutationLock, lock_path_for};
use tempfile::TempDir;

const LEGACY_FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/legacy_pid_lock.sh"
);

fn shell_holder(lock_path: &std::path::Path) -> Child {
    let mut child = Command::new("bash")
        .args([
            "-c",
            "exec 9<>\"$1\"; flock -n 9; printf '%s\\n' \"$$\" > \"$1\"; \
             echo ready; read -r _",
            "holder",
        ])
        .arg(lock_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    assert_eq!(
        BufReader::new(stdout).lines().next().unwrap().unwrap(),
        "ready"
    );
    child
}

fn activation_shell_lock(path: &std::path::Path) -> File {
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    file.try_lock_exclusive().unwrap();
    file.set_len(0).unwrap();
    writeln!(file, "{}", std::process::id()).unwrap();
    file.flush().unwrap();
    file
}

#[cfg(unix)]
fn inode(path: &std::path::Path) -> u64 {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata(path).unwrap().ino()
}

#[test]
fn shell_flock_blocks_rust_then_kill_releases_same_inode() {
    let dir = TempDir::new().unwrap();
    let cache_path = dir.path().join("cache.db");
    let lock_path = lock_path_for(&cache_path);
    let mut holder = shell_holder(&lock_path);
    let held_inode = inode(&lock_path);
    let held_pid = std::fs::read_to_string(&lock_path).unwrap();

    assert!(CacheMutationLock::acquire(&cache_path).is_err());
    assert_eq!(std::fs::read_to_string(&lock_path).unwrap(), held_pid);
    assert_eq!(inode(&lock_path), held_inode);

    holder.kill().unwrap();
    holder.wait().unwrap();
    let rust_holder = CacheMutationLock::acquire(&cache_path).unwrap();
    assert_eq!(inode(rust_holder.path()), held_inode);
    drop(rust_holder);
    assert!(lock_path.is_file());
}

#[test]
fn rust_lock_blocks_shell_flock_without_changing_pid() {
    let dir = TempDir::new().unwrap();
    let cache_path = dir.path().join("cache.db");
    let rust_holder = CacheMutationLock::acquire(&cache_path).unwrap();
    let before = std::fs::read_to_string(rust_holder.path()).unwrap();
    let held_inode = inode(rust_holder.path());

    let rejected = Command::new("flock")
        .args(["-n", rust_holder.path().to_str().unwrap(), "true"])
        .status()
        .unwrap();
    assert!(!rejected.success());
    assert_eq!(std::fs::read_to_string(rust_holder.path()).unwrap(), before);
    assert_eq!(inode(rust_holder.path()), held_inode);
    drop(rust_holder);

    let acquired = Command::new("flock")
        .args(["-n", lock_path_for(&cache_path).to_str().unwrap(), "true"])
        .status()
        .unwrap();
    assert!(acquired.success());
}

#[test]
fn activation_holder_takes_loop_then_run_then_cache_without_deadlock() {
    let dir = TempDir::new().unwrap();
    let loop_path = dir.path().join(".rank_and_push_loop.lock");
    let run_path = dir.path().join(".rank_and_push.lock");
    let cache_path = dir.path().join("cache.db");

    let loop_holder = activation_shell_lock(&loop_path);
    let run_holder = activation_shell_lock(&run_path);
    let cache_holder = CacheMutationLock::acquire(&cache_path).unwrap();

    for path in [&loop_path, &run_path, cache_holder.path()] {
        let contender = Command::new("flock")
            .args(["-n", path.to_str().unwrap(), "true"])
            .status()
            .unwrap();
        assert!(
            !contender.success(),
            "activation lock was not exclusive: {path:?}"
        );
    }
    assert!(CacheMutationLock::acquire(&cache_path).is_err());

    drop(cache_holder);
    drop(run_holder);
    drop(loop_holder);
    for path in [&loop_path, &run_path, &lock_path_for(&cache_path)] {
        let contender = Command::new("flock")
            .args(["-n", path.to_str().unwrap(), "true"])
            .status()
            .unwrap();
        assert!(
            contender.success(),
            "released activation lock stayed held: {path:?}"
        );
        assert!(path.is_file(), "activation removed a persistent lock inode");
    }
}

#[test]
fn legacy_wrapper_check_refuses_live_new_holder_without_mutation() {
    let dir = TempDir::new().unwrap();
    let cache_path = dir.path().join("cache.db");
    let holder = CacheMutationLock::acquire(&cache_path).unwrap();
    let before = std::fs::read_to_string(holder.path()).unwrap();
    let held_inode = inode(holder.path());

    let rejected = Command::new("bash")
        .args([LEGACY_FIXTURE, "wrapper-check"])
        .arg(holder.path())
        .status()
        .unwrap();
    assert_eq!(rejected.code(), Some(3));
    let rejected_cache = Command::new("bash")
        .args([LEGACY_FIXTURE, "cache-reclaim"])
        .arg(holder.path())
        .status()
        .unwrap();
    assert_eq!(rejected_cache.code(), Some(3));
    assert_eq!(std::fs::read_to_string(holder.path()).unwrap(), before);
    assert_eq!(inode(holder.path()), held_inode);
    assert!(!cache_path.exists());
}

#[test]
fn legacy_wrapper_check_then_write_allows_two_contenders() {
    let dir = TempDir::new().unwrap();
    let lock_path = dir.path().join("legacy-run.lock");
    let ready = dir.path().join("ready");
    let go = dir.path().join("go");
    let result = dir.path().join("result");
    let mut children = Vec::new();
    for token in ["one", "two"] {
        children.push(
            Command::new("bash")
                .args([LEGACY_FIXTURE, "wrapper-race"])
                .arg(&lock_path)
                .arg(&ready)
                .arg(&go)
                .arg(&result)
                .arg(token)
                .spawn()
                .unwrap(),
        );
    }

    let deadline = Instant::now() + Duration::from_secs(5);
    while std::fs::read_to_string(&ready).map_or(0, |contents| contents.lines().count()) < 2 {
        assert!(
            Instant::now() < deadline,
            "legacy contenders did not reach barrier"
        );
        std::thread::yield_now();
    }
    std::fs::write(&go, "go\n").unwrap();
    for child in &mut children {
        assert!(child.wait().unwrap().success());
    }
    let mut acquired = std::fs::read_to_string(&result)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    acquired.sort();
    assert_eq!(acquired, ["one", "two"]);
}

#[test]
fn legacy_stale_reclaim_can_unlink_a_kernel_locked_inode() {
    let dir = TempDir::new().unwrap();
    let cache_path = dir.path().join("cache.db");
    let holder = CacheMutationLock::acquire(&cache_path).unwrap();
    let held_inode = inode(holder.path());
    std::fs::write(holder.path(), "not-yet-stamped\n").unwrap();

    let legacy = Command::new("bash")
        .args([LEGACY_FIXTURE, "cache-reclaim"])
        .arg(holder.path())
        .status()
        .unwrap();
    assert!(legacy.success());
    assert_ne!(inode(holder.path()), held_inode);

    drop(holder);
    let new_holder = CacheMutationLock::acquire(&cache_path).unwrap();
    assert_eq!(new_holder.path(), lock_path_for(&cache_path));
}
