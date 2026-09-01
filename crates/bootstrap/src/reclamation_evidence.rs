//! Read-only Forge reclamation evidence captured under the cache lock (#544).

use std::path::{Path, PathBuf};

use serde::Serialize;
use serde_json::Value;
use time::OffsetDateTime;

use crate::cache::{REQUIRED_TRADES_INDEXES, WalletCache};
use crate::error::BootstrapError;

/// Resolve the Forge evidence directory beside the configured cache's parent.
#[must_use]
pub fn eval_results_dir_for_cache(cache_path: &Path) -> PathBuf {
    cache_path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .map_or_else(
            || PathBuf::from("data/eval-results"),
            |parent| parent.join("eval-results"),
        )
}

/// One activation-gate capture. `activation_ready` covers only the two cache
/// invariants owned by this helper: marker absent and required indexes present.
#[derive(Debug, Serialize)]
pub struct ReclamationEvidenceReport {
    pub captured_at_unix: i64,
    pub cache_path: PathBuf,
    pub reclamation_pending: bool,
    pub freelist_pages: i64,
    pub trades_indexes: Vec<String>,
    pub required_trades_indexes: Vec<&'static str>,
    pub missing_required_trades_indexes: Vec<String>,
    pub activation_ready: bool,
    pub latest_purge_status_path: Option<PathBuf>,
    pub latest_purge_status_records: Vec<Value>,
    pub sqlite_tmpdir: SqliteTempEvidence,
}

#[derive(Debug, Serialize)]
pub struct SqliteTempEvidence {
    pub configured_path: Option<PathBuf>,
    pub resolved_path: PathBuf,
    pub device_id: u64,
    pub available_bytes: u64,
}

/// Capture activation evidence through a genuinely read-only SQLite open.
/// The caller must hold [`crate::lock::CacheMutationLock`] before entry.
pub fn capture(
    cache_path: &Path,
    eval_results_dir: &Path,
) -> Result<ReclamationEvidenceReport, BootstrapError> {
    let cache = WalletCache::open_read_only(cache_path)?;
    let cache_evidence = cache.reclamation_evidence()?;
    let (latest_purge_status_path, latest_purge_status_records) =
        latest_purge_status(eval_results_dir)?;
    let sqlite_tmpdir = sqlite_temp_evidence()?;
    let activation_ready = !cache_evidence.reclamation_pending
        && cache_evidence.missing_required_trades_indexes.is_empty();

    Ok(ReclamationEvidenceReport {
        captured_at_unix: OffsetDateTime::now_utc().unix_timestamp(),
        cache_path: std::fs::canonicalize(cache_path)?,
        reclamation_pending: cache_evidence.reclamation_pending,
        freelist_pages: cache_evidence.freelist_pages,
        trades_indexes: cache_evidence.trades_indexes,
        required_trades_indexes: REQUIRED_TRADES_INDEXES.to_vec(),
        missing_required_trades_indexes: cache_evidence.missing_required_trades_indexes,
        activation_ready,
        latest_purge_status_path,
        latest_purge_status_records,
        sqlite_tmpdir,
    })
}

fn latest_purge_status(root: &Path) -> Result<(Option<PathBuf>, Vec<Value>), BootstrapError> {
    let direct_status = root.join("purge_status.jsonl");
    if direct_status.is_file() {
        return Ok((
            Some(direct_status.clone()),
            read_status_records(&direct_status)?,
        ));
    }
    let mut paths = Vec::new();
    let entries = match std::fs::read_dir(root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok((None, Vec::new()));
        }
        Err(error) => return Err(error.into()),
    };
    for entry in entries {
        let entry = entry?;
        if !entry.file_type()?.is_dir() || !entry.file_name().to_string_lossy().starts_with("cron-")
        {
            continue;
        }
        let path = entry.path().join("purge_status.jsonl");
        if path.is_file() {
            paths.push(path);
        }
    }
    paths.sort();
    let Some(path) = paths.pop() else {
        return Ok((None, Vec::new()));
    };
    let records = read_status_records(&path)?;
    Ok((Some(path), records))
}

fn read_status_records(path: &Path) -> Result<Vec<Value>, BootstrapError> {
    Ok(std::fs::read_to_string(path)?
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?)
}

fn sqlite_temp_evidence() -> Result<SqliteTempEvidence, BootstrapError> {
    let configured_path = std::env::var_os("SQLITE_TMPDIR").map(PathBuf::from);
    let effective_path = configured_path
        .as_deref()
        .map_or_else(std::env::temp_dir, Path::to_path_buf);
    let resolved_path = std::fs::canonicalize(effective_path)?;
    let metadata = std::fs::metadata(&resolved_path)?;
    Ok(SqliteTempEvidence {
        configured_path,
        device_id: device_id(&metadata),
        available_bytes: fs2::available_space(&resolved_path)?,
        resolved_path,
    })
}

#[cfg(unix)]
fn device_id(metadata: &std::fs::Metadata) -> u64 {
    use std::os::unix::fs::MetadataExt;
    metadata.dev()
}

#[cfg(not(unix))]
fn device_id(_metadata: &std::fs::Metadata) -> u64 {
    0
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn latest_status_selects_newest_cron_with_records() {
        let dir = TempDir::new().unwrap();
        for (name, stage) in [
            ("cron-20260101T000000Z", "purge"),
            ("cron-20260102T000000Z", "purge-infra"),
        ] {
            let run = dir.path().join(name);
            std::fs::create_dir(&run).unwrap();
            std::fs::write(
                run.join("purge_status.jsonl"),
                format!("{{\"stage\":\"{stage}\",\"exit_code\":0}}\n"),
            )
            .unwrap();
        }

        let (path, records) = latest_purge_status(dir.path()).unwrap();
        assert!(
            path.unwrap()
                .ends_with("cron-20260102T000000Z/purge_status.jsonl")
        );
        assert_eq!(records[0]["stage"], "purge-infra");
    }

    #[test]
    fn direct_status_is_the_current_manual_command_ledger() {
        let dir = TempDir::new().unwrap();
        let old_run = dir.path().join("cron-20260102T000000Z");
        std::fs::create_dir(&old_run).unwrap();
        std::fs::write(
            old_run.join("purge_status.jsonl"),
            "{\"stage\":\"purge-infra\",\"exit_code\":0}\n",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("purge_status.jsonl"),
            "{\"stage\":\"purge\",\"exit_code\":1}\n",
        )
        .unwrap();

        let (path, records) = latest_purge_status(dir.path()).unwrap();
        assert_eq!(path.unwrap(), dir.path().join("purge_status.jsonl"));
        assert_eq!(records[0]["stage"], "purge");
    }
}
