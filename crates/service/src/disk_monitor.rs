//! Free-space guard for the filesystems holding ordinary-service durable state.

use std::collections::HashSet;
use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use tracing::{error, info, warn};

use crate::config::ServiceConfig;
use crate::health::SharedHealth;
use crate::supervisor::{ShutdownPhase, ShutdownReceiver, TaskExit, TaskFailure, TaskResult};

/// Compiled defaults; see `docs/_GLOSSARY.md` configuration defaults.
pub const DISK_FREE_WARN_BYTES: u64 = 15_000_000_000;
pub const DISK_FREE_FLOOR_BYTES: u64 = 5_000_000_000;
pub const DISK_SAMPLE_SECS: u64 = 60;

#[derive(Debug, PartialEq, Eq)]
enum DiskTransition {
    Unchanged,
    Warning,
    Recovered,
}

pub struct DiskMonitor {
    directories: Vec<PathBuf>,
    low: Vec<bool>,
    warned: bool,
}

impl DiskMonitor {
    /// Resolve existing parents without creating files or directories. A new directory inherits
    /// its nearest existing ancestor's filesystem; each device needs only one sample.
    pub fn from_config(config: &ServiceConfig) -> Result<Self> {
        let mut devices = HashSet::new();
        let mut directories = Vec::new();
        for path in [
            &config.source_event_log_path,
            &config.paper_state_db_path,
            &config.event_log_path,
            &config.status_path,
            &config.jsonl_log_path,
        ] {
            let parent = path
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new("."));
            let absolute = std::path::absolute(parent)
                .with_context(|| format!("resolve durable path parent {}", parent.display()))?;
            let directory = absolute
                .ancestors()
                .find(|ancestor| ancestor.exists())
                .context("durable path has no existing ancestor")?;
            let metadata = std::fs::metadata(directory)
                .with_context(|| format!("inspect durable directory {}", directory.display()))?;
            anyhow::ensure!(
                metadata.is_dir(),
                "durable parent {} is not a directory",
                directory.display()
            );
            let directory = std::fs::canonicalize(directory)?;
            #[cfg(unix)]
            let new_device = {
                use std::os::unix::fs::MetadataExt as _;
                devices.insert(metadata.dev())
            };
            #[cfg(not(unix))]
            let new_device = devices.insert(directory.clone());
            if new_device {
                directories.push(directory);
            }
        }
        Ok(Self {
            low: vec![false; directories.len()],
            directories,
            warned: false,
        })
    }

    /// Refuse writable initialization when any durable filesystem is below the floor.
    pub fn check_startup(&self) -> Result<()> {
        self.check_startup_with(|path| fs2::available_space(path))
    }

    fn check_startup_with(
        &self,
        mut available_space: impl FnMut(&Path) -> io::Result<u64>,
    ) -> Result<()> {
        for directory in &self.directories {
            let bytes = available_space(directory).with_context(|| {
                format!(
                    "sample disk free space for {} at startup",
                    directory.display()
                )
            })?;
            anyhow::ensure!(
                bytes >= DISK_FREE_FLOOR_BYTES,
                "startup refused: disk free space for {} is {bytes} bytes, below floor {DISK_FREE_FLOOR_BYTES} bytes",
                directory.display()
            );
        }
        Ok(())
    }

    pub async fn run(self, health: SharedHealth, shutdown: ShutdownReceiver) -> TaskResult {
        self.run_with(health, shutdown, |path| fs2::available_space(path))
            .await
    }

    async fn run_with(
        mut self,
        health: SharedHealth,
        shutdown: ShutdownReceiver,
        mut available_space: impl FnMut(&Path) -> io::Result<u64>,
    ) -> TaskResult {
        let mut ticker = tokio::time::interval(Duration::from_secs(DISK_SAMPLE_SECS));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let stopping = shutdown.wait_for(ShutdownPhase::StopProducers);
        tokio::pin!(stopping);
        info!(sample_secs = DISK_SAMPLE_SECS, directories = ?self.directories, "disk space monitoring started");
        loop {
            tokio::select! {
                biased;
                () = &mut stopping => return Ok(TaskExit::CleanShutdown),
                _ = ticker.tick() => { self.sample_with(&health, &mut available_space)?; }
            }
        }
    }

    fn sample_with(
        &mut self,
        health: &SharedHealth,
        mut available_space: impl FnMut(&Path) -> io::Result<u64>,
    ) -> Result<DiskTransition, TaskFailure> {
        let mut floor = None;
        for (directory, low) in self.directories.iter().zip(&mut self.low) {
            match available_space(directory) {
                Ok(bytes) => {
                    *low = bytes < DISK_FREE_WARN_BYTES;
                    if bytes < DISK_FREE_FLOOR_BYTES {
                        floor = Some((directory, bytes));
                    }
                }
                Err(error) => {
                    warn!(directory = %directory.display(), error = %error, "disk free-space sample failed; retrying next tick");
                }
            }
        }
        let low = self.low.iter().any(|low| *low);
        health
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .disk_low = low;
        let transition = match (self.warned, low) {
            (false, true) => {
                error!(warn_bytes = DISK_FREE_WARN_BYTES, directories = ?self.directories, "disk free space below warning threshold");
                DiskTransition::Warning
            }
            (true, false) => {
                info!(
                    warn_bytes = DISK_FREE_WARN_BYTES,
                    "disk free space recovered"
                );
                DiskTransition::Recovered
            }
            _ => DiskTransition::Unchanged,
        };
        self.warned = low;
        if let Some((directory, bytes)) = floor {
            error!(directory = %directory.display(), available_bytes = bytes, floor_bytes = DISK_FREE_FLOOR_BYTES, "disk free space below floor; requesting coordinated shutdown");
            return Err(TaskFailure::typed(format!(
                "disk free space for {} is {bytes} bytes, below floor {DISK_FREE_FLOOR_BYTES} bytes",
                directory.display()
            )));
        }
        Ok(transition)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::health::{new_shared_health, readiness_issues};
    use crate::supervisor::{ShutdownController, TaskName, TaskSupervisor};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use time::OffsetDateTime;
    use tokio::time::Instant;

    fn config_at(directory: &Path) -> ServiceConfig {
        ServiceConfig {
            source_event_log_path: directory.join("source/events.log"),
            paper_state_db_path: directory.join("state/paper.db"),
            event_log_path: directory.join("paper/events.log"),
            status_path: directory.join("status/status.json"),
            jsonl_log_path: directory.join("jsonl/service.jsonl"),
            status_interval_secs: 0,
            ..ServiceConfig::default()
        }
    }

    #[test]
    fn disk_monitor_deduplicates_all_durable_parents_without_writes() {
        let directory = tempfile::tempdir().unwrap();
        let config = config_at(directory.path());
        let monitor = DiskMonitor::from_config(&config).unwrap();
        assert_eq!(
            monitor.directories,
            vec![directory.path().canonicalize().unwrap()]
        );
        assert!(
            std::fs::read_dir(directory.path())
                .unwrap()
                .next()
                .is_none()
        );
    }

    #[test]
    fn disk_monitor_checks_every_configured_durable_parent() {
        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("not-a-directory");
        std::fs::write(&file, []).unwrap();
        let setters: [fn(&mut ServiceConfig, PathBuf); 5] = [
            |config, path| config.source_event_log_path = path,
            |config, path| config.paper_state_db_path = path,
            |config, path| config.event_log_path = path,
            |config, path| config.status_path = path,
            |config, path| config.jsonl_log_path = path,
        ];
        for set_path in setters {
            let mut config = config_at(directory.path());
            set_path(&mut config, file.join("durable-file"));
            assert!(DiskMonitor::from_config(&config).is_err());
        }
    }

    #[test]
    fn disk_checks_every_filesystem_and_recovers_only_when_all_are_healthy() {
        let mut monitor = DiskMonitor {
            directories: vec![
                PathBuf::from("first-device"),
                PathBuf::from("second-device"),
            ],
            low: vec![false; 2],
            warned: false,
        };
        let mut sampled = Vec::new();
        assert!(
            monitor
                .check_startup_with(|path| {
                    sampled.push(path.to_owned());
                    Ok(if path == Path::new("second-device") {
                        DISK_FREE_FLOOR_BYTES - 1
                    } else {
                        DISK_FREE_WARN_BYTES
                    })
                })
                .is_err()
        );
        assert_eq!(sampled, monitor.directories);
        let health = new_shared_health(false);
        assert_eq!(
            monitor
                .sample_with(&health, |_| Ok(DISK_FREE_WARN_BYTES - 1))
                .unwrap(),
            DiskTransition::Warning
        );
        assert_eq!(
            monitor
                .sample_with(&health, |path| {
                    if path == Path::new("second-device") {
                        Err(io::Error::other("injected sample error"))
                    } else {
                        Ok(DISK_FREE_WARN_BYTES)
                    }
                })
                .unwrap(),
            DiskTransition::Unchanged
        );
        assert!(health.lock().unwrap().disk_low);
        assert_eq!(
            monitor
                .sample_with(&health, |_| Ok(DISK_FREE_WARN_BYTES + 1))
                .unwrap(),
            DiskTransition::Recovered
        );
        assert!(!health.lock().unwrap().disk_low);
    }

    #[test]
    fn disk_startup_refuses_below_floor_with_injected_space() {
        let directory = tempfile::tempdir().unwrap();
        let monitor = DiskMonitor::from_config(&config_at(directory.path())).unwrap();
        let error = monitor
            .check_startup_with(|_| Ok(DISK_FREE_FLOOR_BYTES - 1))
            .unwrap_err();
        assert!(error.to_string().contains("startup refused"));
        assert!(
            error
                .to_string()
                .contains(&directory.path().display().to_string())
        );
        monitor
            .check_startup_with(|_| Ok(DISK_FREE_FLOOR_BYTES))
            .unwrap();
        assert!(
            std::fs::read_dir(directory.path())
                .unwrap()
                .next()
                .is_none()
        );
    }

    #[test]
    fn disk_threshold_transitions_warn_once_recover_and_preserve_low_on_sample_error() {
        let directory = tempfile::tempdir().unwrap();
        let mut monitor = DiskMonitor::from_config(&config_at(directory.path())).unwrap();
        let health = new_shared_health(false);
        for (bytes, transition, low) in [
            (DISK_FREE_WARN_BYTES, DiskTransition::Unchanged, false),
            (DISK_FREE_WARN_BYTES - 1, DiskTransition::Warning, true),
            (DISK_FREE_FLOOR_BYTES, DiskTransition::Unchanged, true),
        ] {
            assert_eq!(
                monitor.sample_with(&health, |_| Ok(bytes)).unwrap(),
                transition
            );
            assert_eq!(health.lock().unwrap().disk_low, low);
        }
        assert_eq!(
            monitor
                .sample_with(&health, |_| Err(io::Error::other("injected sample error")))
                .unwrap(),
            DiskTransition::Unchanged
        );
        assert!(health.lock().unwrap().disk_low);
        assert_eq!(
            monitor
                .sample_with(&health, |_| Ok(DISK_FREE_WARN_BYTES + 1))
                .unwrap(),
            DiskTransition::Recovered
        );
        let now = OffsetDateTime::from_unix_timestamp(10_000).unwrap();
        assert!(!health.lock().unwrap().disk_low);
        let ready_before = readiness_issues(&health.lock().unwrap(), now, Instant::now());
        monitor
            .sample_with(&health, |_| Ok(DISK_FREE_WARN_BYTES - 1))
            .unwrap();
        // The warning is an alarm, not a readiness condition: the service still works.
        assert!(health.lock().unwrap().disk_low);
        assert_eq!(
            readiness_issues(&health.lock().unwrap(), now, Instant::now()),
            ready_before
        );
    }

    #[tokio::test(start_paused = true)]
    async fn disk_floor_requests_critical_owner_shutdown() {
        let directory = tempfile::tempdir().unwrap();
        let monitor = DiskMonitor::from_config(&config_at(directory.path())).unwrap();
        let health = new_shared_health(false);
        let status = health.lock().unwrap().task_status.clone();
        let mut supervisor = TaskSupervisor::new(status);
        let (shutdown, _) = ShutdownController::new();
        supervisor.spawn(
            TaskName::DiskMonitor,
            monitor.run_with(health.clone(), shutdown.subscribe(), |_| {
                Ok(DISK_FREE_FLOOR_BYTES - 1)
            }),
        );
        let event = supervisor.observe_next().await.unwrap();
        assert_eq!(event.name, TaskName::DiskMonitor);
        assert!(event.initiates_shutdown());
        assert!(health.lock().unwrap().disk_low);
        assert!(supervisor.status().critical_failed());
    }

    #[tokio::test(start_paused = true)]
    async fn disk_task_samples_immediately_and_every_minute_with_status_ticks_off() {
        let directory = tempfile::tempdir().unwrap();
        let config = config_at(directory.path());
        assert_eq!(config.status_interval_secs, 0);
        let monitor = DiskMonitor::from_config(&config).unwrap();
        let health = new_shared_health(false);
        let status = health.lock().unwrap().task_status.clone();
        let mut supervisor = TaskSupervisor::new(status.clone());
        let (shutdown, _) = ShutdownController::new();
        let samples = Arc::new(AtomicUsize::new(0));
        let sampled = samples.clone();
        supervisor.spawn(
            TaskName::DiskMonitor,
            monitor.run_with(health.clone(), shutdown.subscribe(), move |_| {
                let sample = sampled.fetch_add(1, Ordering::SeqCst);
                if sample == 0 {
                    Err(io::Error::other("injected first sample error"))
                } else {
                    Ok(DISK_FREE_WARN_BYTES - 1)
                }
            }),
        );
        tokio::task::yield_now().await;
        assert_eq!(samples.load(Ordering::SeqCst), 1);
        assert!(!health.lock().unwrap().disk_low);
        tokio::time::advance(Duration::from_secs(DISK_SAMPLE_SECS - 1)).await;
        tokio::task::yield_now().await;
        assert_eq!(samples.load(Ordering::SeqCst), 1);
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(samples.load(Ordering::SeqCst), 2);
        assert!(health.lock().unwrap().disk_low);
        tokio::time::advance(Duration::from_secs(DISK_SAMPLE_SECS)).await;
        tokio::task::yield_now().await;
        assert_eq!(samples.load(Ordering::SeqCst), 3);
        status.advance_phase(ShutdownPhase::StopProducers);
        shutdown.advance(ShutdownPhase::StopProducers);
        assert!(supervisor.observe_next().await.unwrap().failure.is_none());
        tokio::time::advance(Duration::from_secs(DISK_SAMPLE_SECS)).await;
        assert_eq!(samples.load(Ordering::SeqCst), 3);
    }
}
