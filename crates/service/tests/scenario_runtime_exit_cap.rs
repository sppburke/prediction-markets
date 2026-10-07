#![cfg(feature = "scenario")]
#![forbid(unsafe_code)]

use std::fs::File;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};

/// The binary times its own runtime teardown, so the bound does not depend on how promptly this
/// test process is scheduled; the outer watchdog only catches a teardown that never ends.
fn check_exit_cap(kind: &str) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let stdout = directory.path().join("stdout");
    let stderr = directory.path().join("stderr");
    let mut child = Command::new(env!("CARGO_BIN_EXE_pe-service"))
        .args(["--scenario-runtime-exit-cap", kind])
        .env_clear()
        .env("TOKIO_WORKER_THREADS", "2")
        .stdout(Stdio::from(File::create(&stdout)?))
        .stderr(Stdio::from(File::create(&stderr)?))
        .spawn()?;
    let watchdog = Instant::now() + Duration::from_secs(60);
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if Instant::now() >= watchdog {
            child.kill()?;
            child.wait()?;
            anyhow::bail!("{kind}: synchronous work kept the service alive after main finished");
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    assert!(status.success(), "{}", std::fs::read_to_string(&stderr)?);
    let output = std::fs::read_to_string(&stdout)?;
    assert!(
        output.lines().any(|line| line == "scenario main finished"),
        "{output}"
    );
    let teardown_ms: u64 = output
        .lines()
        .find_map(|line| line.strip_prefix("scenario runtime teardown_ms="))
        .context("teardown measurement")?
        .parse()?;
    // The stuck task holds the runtime for the whole bound, and the bound ends the wait.
    assert!(
        (9_900..=10_500).contains(&teardown_ms),
        "{kind}: runtime teardown took {teardown_ms} ms"
    );
    Ok(())
}

#[test]
fn runtime_exit_cap_bounds_blocking_work() -> Result<()> {
    check_exit_cap("blocking")
}

#[test]
fn runtime_exit_cap_bounds_async_synchronous_work() -> Result<()> {
    check_exit_cap("async")
}
