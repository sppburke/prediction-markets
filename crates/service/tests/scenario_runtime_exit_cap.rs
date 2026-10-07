#![cfg(feature = "scenario")]
#![forbid(unsafe_code)]

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};

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
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut finished = None;
    loop {
        if finished.is_none()
            && BufReader::new(File::open(&stdout)?)
                .lines()
                .collect::<std::io::Result<Vec<_>>>()?
                .iter()
                .any(|line| line == "scenario main finished")
        {
            finished = Some(Instant::now());
        }
        if let Some(status) = child.try_wait()? {
            assert!(status.success(), "{}", std::fs::read_to_string(&stderr)?);
            let elapsed = finished.context("async main completion marker")?.elapsed();
            // Process observation includes scheduling and polling beyond the runtime's exact bound.
            assert!(
                elapsed <= Duration::from_millis(10_500),
                "{kind}: {elapsed:?}"
            );
            return Ok(());
        }
        if Instant::now() >= deadline {
            child.kill()?;
            child.wait()?;
            anyhow::bail!("{kind} synchronous work kept the service alive after main finished");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn runtime_exit_cap_bounds_blocking_work() -> Result<()> {
    check_exit_cap("blocking")
}

#[test]
fn runtime_exit_cap_bounds_async_synchronous_work() -> Result<()> {
    check_exit_cap("async")
}
