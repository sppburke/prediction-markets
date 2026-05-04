//! Dual-layer tracing setup: human-readable stderr + machine-readable JSONL file.

use std::fs::OpenOptions;
use std::path::Path;

use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt as _, util::SubscriberInitExt as _};

/// Initialise the global tracing subscriber with two layers:
///
/// 1. **stderr** — compact human-readable output, filtered by `RUST_LOG` or `default_filter`.
/// 2. **JSONL file** — one JSON object per line written to `jsonl_path`, same filter.
///
/// Each JSONL line includes at minimum `ts` (RFC-3339 UTC) and `level` fields; structured
/// fields added via `tracing::info!(kind = "paper_fill", ...)` appear as top-level JSON keys.
///
/// See `docs/_GLOSSARY.md` "JSONL observability sidecar schema" for the per-kind field tables.
pub fn setup(jsonl_path: &Path, default_filter: &str) -> anyhow::Result<()> {
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(jsonl_path)
        .map_err(|e| anyhow::anyhow!("open jsonl log {}: {e}", jsonl_path.display()))?;

    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_filter));

    let stderr_layer = fmt::layer().with_writer(std::io::stderr).compact();
    let json_layer = fmt::layer().json().with_writer(file).flatten_event(true);

    tracing_subscriber::registry()
        .with(filter)
        .with(stderr_layer)
        .with(json_layer)
        .try_init()
        .map_err(|e| anyhow::anyhow!("tracing init: {e}"))?;

    Ok(())
}
