//! Agent-friendly, bounded JSONL tracing (issue #184; rotation + split sinks).
//!
//! Three JSONL sinks, none unbounded:
//! - **stderr** (→ journald) — full stream at the `RUST_LOG` / `default_filter` level.
//! - **`<stem>.<date>.jsonl`** — the full stream on disk, rotated **daily**, keeping the most
//!   recent `retention_days` files (so per-file greps are small and total disk is bounded).
//! - **`errors.<date>.jsonl`** — **WARN+ERROR only**, the clean "what broke" tape (no INFO
//!   chatter like `signal did not produce order`), rotated daily the same way.
//!
//! Paired with the [`crate::status_writer`] `status.json` snapshot, an agent reads
//! `status.json` for current health, `errors.<date>.jsonl` for problems, and the dated full
//! stream for detail — each focused and bounded, none a multi-GB firehose.
//!
//! See `docs/_GLOSSARY.md` "JSONL observability sidecar schema" for the per-kind field tables.

use std::io::{self, Write};
use std::path::Path;

use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::filter::LevelFilter;
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
/// `jsonl_path` is the **base** path: its directory holds the rolling files and its file stem
/// names the full-stream files (`<stem>.<date>.jsonl`). `retention_days` bounds how many dated
/// files each rolling sink keeps (clamped to ≥ 1 so a misconfig cannot disable file logging).
///
/// Returns the non-blocking [`WorkerGuard`]s — the caller **must** keep them alive for the
/// process lifetime; dropping them flushes and stops the writer threads (losing buffered lines).
pub fn setup(
    jsonl_path: &Path,
    default_filter: &str,
    retention_days: usize,
) -> anyhow::Result<Vec<WorkerGuard>> {
    let dir = jsonl_path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let stem = jsonl_path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("paper")
        .to_string();
    let keep = retention_days.max(1);

    let full_appender = RollingFileAppender::builder()
        .rotation(Rotation::DAILY)
        .filename_prefix(&stem)
        .filename_suffix("jsonl")
        .max_log_files(keep)
        .build(dir)
        .map_err(|e| anyhow::anyhow!("build full log appender in {}: {e}", dir.display()))?;
    let (full_nb, full_guard) = tracing_appender::non_blocking(full_appender);

    let err_appender = RollingFileAppender::builder()
        .rotation(Rotation::DAILY)
        .filename_prefix("errors")
        .filename_suffix("jsonl")
        .max_log_files(keep)
        .build(dir)
        .map_err(|e| anyhow::anyhow!("build errors log appender in {}: {e}", dir.display()))?;
    let (err_nb, err_guard) = tracing_appender::non_blocking(err_appender);

    // Per-layer filters (not a single global filter): stderr + the full stream honour
    // `RUST_LOG`/`default_filter`; `errors.jsonl` always captures WARN+ERROR regardless of
    // `RUST_LOG`, so the safety-net tape can never be silenced by a verbosity setting.
    let env =
        || EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_filter));

    let stderr_layer = fmt::layer()
        .with_writer(|| BlockingWriter(std::io::stderr))
        .json()
        .flatten_event(true)
        .with_filter(env());
    let full_layer = fmt::layer()
        .json()
        .with_writer(full_nb)
        .flatten_event(true)
        .with_filter(env());
    let err_layer = fmt::layer()
        .json()
        .with_writer(err_nb)
        .flatten_event(true)
        .with_filter(LevelFilter::WARN);

    tracing_subscriber::registry()
        .with(stderr_layer)
        .with(full_layer)
        .with(err_layer)
        .try_init()
        .map_err(|e| anyhow::anyhow!("tracing init: {e}"))?;

    Ok(vec![full_guard, err_guard])
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
