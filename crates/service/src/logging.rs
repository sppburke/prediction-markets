//! JSONL tracing to stderr (→ journald), with WARN+ERROR always enabled.
//!
//! See `docs/_GLOSSARY.md` "JSONL observability sidecar schema" for the per-kind field tables.

use std::io::{self, Write};

use tracing_subscriber::filter::{FilterExt as _, LevelFilter};
use tracing_subscriber::{
    EnvFilter, Layer as _, fmt, layer::SubscriberExt as _, util::SubscriberInitExt as _,
};

struct BlockingWriter<F>(F);

impl<W: Write, F: Fn() -> W> Write for BlockingWriter<F> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        pe_event_log::run_blocking(|| (self.0)().write(buf))
    }

    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        pe_event_log::run_blocking(|| (self.0)().write_all(buf))
    }

    fn flush(&mut self) -> io::Result<()> {
        pe_event_log::run_blocking(|| (self.0)().flush())
    }
}

/// Initialise the global tracing subscriber.
///
/// `RUST_LOG` overrides `default_filter`; WARN+ERROR can never be silenced by either.
/// No log files or background writers are created.
pub fn setup(default_filter: &str) -> anyhow::Result<()> {
    let env = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_filter));

    let stderr_layer = fmt::layer()
        .with_writer(|| BlockingWriter(std::io::stderr))
        .json()
        .flatten_event(true)
        .with_filter(env.or(LevelFilter::WARN));

    tracing_subscriber::registry()
        .with(stderr_layer)
        .try_init()
        .map_err(|e| anyhow::anyhow!("tracing init: {e}"))?;

    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    struct InjectedWriter(Arc<Mutex<Vec<u8>>>);

    impl Write for InjectedWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().write(buf)
        }

        fn flush(&mut self) -> io::Result<()> {
            self.0.lock().unwrap().flush()
        }
    }

    #[test]
    fn stderr_filter_preserves_warnings_and_json_format() {
        const CHILD: &str = "PE_TEST_LOGGING_CHILD";
        if std::env::var_os(CHILD).is_some() {
            setup("info").unwrap();
            tracing::debug!(target: "pe_service", sample = 42, "debug sample");
            tracing::info!(target: "pe_service", sample = 42, "info sample");
            tracing::warn!(target: "pe_service", sample = 42, "warn sample");
            tracing::error!(target: "pe_service", sample = 42, "error sample");
            return;
        }

        for (filter, levels) in [
            (None, vec!["INFO", "WARN", "ERROR"]),
            (Some("off"), vec!["WARN", "ERROR"]),
            (Some("error"), vec!["WARN", "ERROR"]),
            (Some("debug,pe_service=off"), vec!["WARN", "ERROR"]),
            (
                Some("off,pe_service=debug"),
                vec!["DEBUG", "INFO", "WARN", "ERROR"],
            ),
        ] {
            let directory = tempfile::tempdir().unwrap();
            let mut command = std::process::Command::new(std::env::current_exe().unwrap());
            command
                .args([
                    "logging::tests::stderr_filter_preserves_warnings_and_json_format",
                    "--exact",
                    "--nocapture",
                ])
                .current_dir(directory.path())
                .env(CHILD, "1")
                .env_remove("RUST_LOG");
            if let Some(filter) = filter {
                command.env("RUST_LOG", filter);
            }
            let output = command.output().unwrap();
            assert!(output.status.success(), "{filter:?}: {output:?}");
            assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
            let events = String::from_utf8(output.stderr)
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
                .collect::<Vec<_>>();
            assert_eq!(
                events
                    .iter()
                    .map(|event| event["level"].as_str().unwrap())
                    .collect::<Vec<_>>(),
                levels,
                "{filter:?}"
            );
            for event in events {
                assert_eq!(event["sample"], 42);
                assert!(event["message"].as_str().unwrap().ends_with(" sample"));
                assert!(event.get("fields").is_none());
            }
            assert!(
                std::fs::read_dir(directory.path())
                    .unwrap()
                    .next()
                    .is_none()
            );
        }
    }

    #[test]
    fn blocking_stderr_write_preserves_runtime_progress() {
        stderr_preserves_runtime_progress(0);
    }

    #[test]
    fn blocking_stderr_write_all_preserves_runtime_progress() {
        stderr_preserves_runtime_progress(1);
    }

    #[test]
    fn blocking_stderr_flush_preserves_runtime_progress() {
        stderr_preserves_runtime_progress(2);
    }

    fn stderr_preserves_runtime_progress(operation: u8) {
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let holder = Arc::clone(&bytes);
        crate::runtime_progress::assert_progress(
            move |entered, release| {
                let _guard = holder.lock().unwrap();
                entered.send(()).unwrap();
                release.recv().unwrap();
            },
            move || {
                let mut writer = BlockingWriter(|| InjectedWriter(Arc::clone(&bytes)));
                match operation {
                    0 => {
                        assert_eq!(writer.write(b"line").unwrap(), 4);
                    }
                    1 => {
                        writer.write_all(b"line").unwrap();
                    }
                    _ => {
                        writer.flush().unwrap();
                    }
                }
            },
        );
    }
}
