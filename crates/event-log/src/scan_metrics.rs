//! Development-only counts of complete event-log walks by canonical path.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

static WALKS: OnceLock<Mutex<HashMap<PathBuf, u64>>> = OnceLock::new();

pub(crate) fn record(path: &Path) {
    let walks = WALKS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut counts = walks
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    *counts.entry(path.to_path_buf()).or_default() += 1;
}

/// Return the count of full walks for a canonical source-log path in this process.
pub fn count(path: &Path) -> std::io::Result<u64> {
    let canonical = std::fs::canonicalize(path)?;
    let walks = WALKS.get_or_init(|| Mutex::new(HashMap::new()));
    let counts = walks
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    Ok(*counts.get(&canonical).unwrap_or(&0))
}

static DECODED: OnceLock<Mutex<HashMap<PathBuf, u64>>> = OnceLock::new();
pub(crate) fn record_decoded(path: &Path) {
    let mut counts = DECODED
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    *counts.entry(path.to_path_buf()).or_default() += 1;
}

/// Verified frame decodes in scanner walks, excluding raw-prefix hashing and receipt metadata.
pub fn decoded_count(path: &Path) -> std::io::Result<u64> {
    let canonical = std::fs::canonicalize(path)?;
    let counts = DECODED
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    Ok(*counts.get(&canonical).unwrap_or(&0))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::time::Duration;

    use crate::{Reader, Scanner, Writer};

    #[test]
    fn counts_full_walks_but_not_tail_reader_open() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source.log");
        drop(Writer::open(&path).unwrap());
        let before = super::count(&path).unwrap();
        Scanner::verify(&path).unwrap();
        assert_eq!(super::count(&path).unwrap(), before + 1);
        drop(Reader::replay(&path).unwrap());
        assert_eq!(super::count(&path).unwrap(), before + 3);
        drop(Reader::tail(&path, Duration::from_millis(1)).unwrap());
        assert_eq!(super::count(&path).unwrap(), before + 3);
    }
}
