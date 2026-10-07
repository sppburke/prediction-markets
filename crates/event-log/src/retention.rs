//! Durable authority for intentionally retired source-log bytes.

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use blake3::Hash;
use pe_core_types::{EventSeq, WalletAddress};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{EventEnvelope, HEADER_LEN, LogError, LogTailBinding, Reader};

pub const RETENTION_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetentionBoundary {
    pub sequence: EventSeq,
    pub offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetentionPin {
    pub sequence: EventSeq,
    pub offset: u64,
    #[serde(with = "crate::envelope::hex_hash")]
    pub hash: Hash,
    #[serde(with = "crate::envelope::hex_hash")]
    pub predecessor_hash: Hash,
    pub reducer: bool,
    pub wallet: Option<WalletAddress>,
}

/// Pathless capture, rebound to the configured canonical source path when checked.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TailBinding {
    pub physical_tail: u64,
    pub last_sequence: Option<EventSeq>,
    #[serde(with = "crate::envelope::hex_hash")]
    pub last_hash: Hash,
}

impl From<&LogTailBinding> for TailBinding {
    fn from(binding: &LogTailBinding) -> Self {
        Self {
            physical_tail: binding.physical_tail,
            last_sequence: binding.last_sequence,
            last_hash: binding.last_hash,
        }
    }
}

impl TailBinding {
    pub fn resolve(&self, path: &Path) -> Result<LogTailBinding, LogError> {
        Ok(LogTailBinding {
            path: std::fs::canonicalize(path)?,
            physical_tail: self.physical_tail,
            last_sequence: self.last_sequence,
            last_hash: self.last_hash,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedEntry {
    pub epoch: u64,
    pub frame_count: u64,
    pub length: u64,
    #[serde(with = "crate::envelope::hex_hash")]
    pub hash: Hash,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetentionAuthority {
    pub format_version: u32,
    pub epoch: u64,
    /// Unix seconds at the last advance.
    pub advanced_at: i64,
    pub boundary: RetentionBoundary,
    #[serde(with = "crate::envelope::hex_hash")]
    pub chain_head: Hash,
    pub pins: Vec<RetentionPin>,
    pub retained_tail: TailBinding,
    pub feed: Vec<FeedEntry>,
}

#[derive(Debug, Error)]
pub enum RetentionError {
    #[error("invalid retention authority: {0}")]
    Invalid(String),
    #[error("retention authority I/O: {0}")]
    Io(#[from] io::Error),
}

/// A rename commits the visible epoch even if its subsequent directory sync fails.
#[derive(Debug, Error)]
#[error("retention publication failed (visible={visible}): {source}")]
pub struct RetentionWriteError {
    pub visible: bool,
    #[source]
    pub source: RetentionError,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckedAuthority {
    authority: RetentionAuthority,
    #[serde(with = "crate::envelope::hex_hash")]
    checksum: Hash,
}

pub(crate) fn suffixed(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(suffix);
    PathBuf::from(name)
}

pub fn retention_path(log: &Path) -> PathBuf {
    suffixed(log, ".retention")
}

impl RetentionAuthority {
    pub fn load(log: &Path) -> Result<Option<Self>, RetentionError> {
        let bytes = match std::fs::read(retention_path(log)) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return match std::fs::symlink_metadata(retention_path(log)) {
                    Err(missing) if missing.kind() == io::ErrorKind::NotFound => Ok(None),
                    _ => Err(error.into()),
                };
            }
            Err(error) => return Err(error.into()),
        };
        let checked: CheckedAuthority = serde_json::from_slice(&bytes)
            .map_err(|error| RetentionError::Invalid(error.to_string()))?;
        let canonical = serde_json::to_vec(&checked.authority)
            .map_err(|error| RetentionError::Invalid(error.to_string()))?;
        if blake3::hash(&canonical) != checked.checksum {
            return Err(RetentionError::Invalid("checksum mismatch".into()));
        }
        checked.authority.validate()?;
        Ok(Some(checked.authority))
    }

    pub fn validate(&self) -> Result<(), RetentionError> {
        let invalid = |message: &str| RetentionError::Invalid(message.into());
        if self.format_version != RETENTION_FORMAT_VERSION || self.epoch == 0 {
            return Err(invalid("unsupported version or zero epoch"));
        }
        if self.boundary.sequence.0 == 0
            || self.boundary.offset <= HEADER_LEN
            || self.retained_tail.physical_tail <= self.boundary.offset
            || self
                .retained_tail
                .last_sequence
                .is_none_or(|seq| seq < self.boundary.sequence)
        {
            return Err(invalid("invalid boundary or retained tail"));
        }
        let mut previous = None;
        for pin in &self.pins {
            if pin.sequence >= self.boundary.sequence
                || pin.offset < HEADER_LEN
                || pin.offset >= self.boundary.offset
                || previous.is_some_and(|(seq, offset)| pin.sequence <= seq || pin.offset <= offset)
                || (pin.wallet.is_some() && !pin.reducer)
                || (pin.sequence.0 + 1 == self.boundary.sequence.0 && pin.hash != self.chain_head)
            {
                return Err(invalid("invalid or unsorted pins"));
            }
            previous = Some((pin.sequence, pin.offset));
        }
        let mut previous_epoch = 0;
        for entry in &self.feed {
            if entry.epoch <= previous_epoch
                || entry.epoch > self.epoch
                || entry.frame_count == 0
                || entry.length == 0
            {
                return Err(invalid("invalid or unsorted feed epochs"));
            }
            previous_epoch = entry.epoch;
        }
        Ok(())
    }

    pub fn write(&self, log: &Path) -> Result<(), RetentionWriteError> {
        let before = |source| RetentionWriteError {
            visible: false,
            source,
        };
        self.validate().map_err(before)?;
        let canonical = serde_json::to_vec(self)
            .map_err(|error| before(RetentionError::Invalid(error.to_string())))?;
        let bytes = serde_json::to_vec(&CheckedAuthority {
            authority: self.clone(),
            checksum: blake3::hash(&canonical),
        })
        .map_err(|error| before(RetentionError::Invalid(error.to_string())))?;
        let destination = retention_path(log);
        let temporary = suffixed(&destination, ".tmp");
        let result = (|| -> io::Result<()> {
            let mut file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&temporary)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            std::fs::rename(&temporary, &destination)
        })();
        if let Err(error) = result {
            let _ = std::fs::remove_file(&temporary);
            return Err(before(error.into()));
        }
        Self::sync_directory(log).map_err(|source| RetentionWriteError {
            visible: true,
            source,
        })
    }

    /// Finish durability after a publication whose rename was already visible.
    pub fn sync_directory(log: &Path) -> Result<(), RetentionError> {
        sync_parent(&retention_path(log)).map_err(Into::into)
    }

    pub fn pin(&self, sequence: EventSeq) -> Option<&RetentionPin> {
        self.pins
            .binary_search_by_key(&sequence, |pin| pin.sequence)
            .ok()
            .map(|i| &self.pins[i])
    }

    pub fn verify_pin(
        &self,
        path: &Path,
        pin: &RetentionPin,
    ) -> Result<(EventEnvelope, u64), LogError> {
        let (envelope, end) =
            Reader::read_at(path, pin.offset, pin.sequence, pin.predecessor_hash)?;
        if envelope.this_hash != pin.hash || end > self.boundary.offset {
            return Err(RetentionError::Invalid("pinned frame mismatch".into()).into());
        }
        Ok((envelope, end))
    }

    pub fn verify_pins(&self, path: &Path) -> Result<(), LogError> {
        for pin in &self.pins {
            self.verify_pin(path, pin)?;
        }
        Ok(())
    }

    pub(crate) fn authenticate_binding(
        &self,
        path: &Path,
        binding: &LogTailBinding,
    ) -> Result<(), LogError> {
        if binding.physical_tail > self.boundary.offset {
            return Ok(());
        }
        let pin = binding
            .last_sequence
            .and_then(|seq| self.pin(seq))
            .ok_or_else(|| RetentionError::Invalid("binding is not pinned".into()))?;
        if std::fs::canonicalize(path)? != binding.path || binding.last_hash != pin.hash {
            return Err(RetentionError::Invalid("pinned binding mismatch".into()).into());
        }
        let (_, end) = self.verify_pin(path, pin)?;
        if end != binding.physical_tail {
            return Err(
                RetentionError::Invalid("pinned binding end offset mismatch".into()).into(),
            );
        }
        Ok(())
    }
}

pub(crate) fn sync_parent(path: &Path) -> io::Result<()> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    File::open(parent)?.sync_all()
}
