//! Durable source-log checkpoint artifacts and their cross-process publication authority.
//!
//! Source-log verification and candidate serialization precede the persistent checkpoint lock. A
//! serialized candidate keeps its authority and capture time through every publication retry.

mod runtime;
pub use runtime::{
    CHECKPOINT_PUBLISH_SECS, CHECKPOINT_RETRY_SECS, CheckpointJobSlot, SourceCheckpointOwner,
};
#[cfg(feature = "scenario")]
pub use runtime::{CheckpointOwnerHooks, cli_owner_hooks};

use std::fs::{File, OpenOptions};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use pe_event_log::LogTailBinding;
use serde::{Deserialize, Serialize, de::IgnoredAny};
use thiserror::Error;
use tracing::info;

use crate::risk_inputs::{SourceFrameMetadata, SourceReceiptIndex};
use crate::source_log_boot::ACTIVITY_REDUCER_VERSION;
use crate::trade_poller::{ActivityCandidates, DailyBoundaryCandidates};

pub(crate) const CHECKPOINT_HEADER_LEN: usize = 65;

#[derive(Serialize, Deserialize)]
pub(crate) struct CheckpointData {
    pub(crate) format_version: u32,
    pub(crate) scanner_version: u32,
    pub(crate) reducer_version: u32,
    pub(crate) financial_era: bool,
    pub(crate) activation: LogTailBinding,
    pub(crate) tail: LogTailBinding,
    pub(crate) prefix_blake3: String,
    pub(crate) receipts: Vec<SourceFrameMetadata>,
    pub(crate) activity: ActivityCandidates,
    pub(crate) daily_boundary: Option<DailyBoundaryCandidates>,
}

/// The checksum identifies the exact artifact checked by a preparation, even when reducer maps
/// serialize in different orders at the same tail.
pub(crate) struct LoadedCheckpoint {
    pub(crate) data: CheckpointData,
    pub(crate) checksum: [u8; 64],
}

#[derive(Deserialize)]
struct HeaderView {
    format_version: u32,
    scanner_version: u32,
    reducer_version: u32,
    financial_era: bool,
    activation: LogTailBinding,
    tail: LogTailBinding,
    prefix_blake3: String,
    receipts: Vec<SourceFrameMetadata>,
    // Publication must not allocate the reducer projections.
    #[serde(rename = "activity")]
    _activity: IgnoredAny,
    daily_boundary: Option<IgnoredAny>,
}

enum ArtifactCheck {
    Absent,
    Invalid,
    Inapplicable,
    Valid(Box<HeaderView>),
}

/// The checkpoint path is unchanged from the original boot loader.
#[must_use]
pub fn checkpoint_path(source_log: &Path) -> PathBuf {
    suffixed(source_log, ".boot-checkpoint")
}

fn suffixed(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_owned();
    value.push(suffix);
    PathBuf::from(value)
}

fn record_path(source_log: &Path) -> PathBuf {
    suffixed(&checkpoint_path(source_log), ".invalidation")
}

/// Durable authority for all verification started under one generation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvalidationRecord {
    pub generation: u64,
    pub active: bool,
}

/// Undecodable records are quarantines, never an absent or inactive record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Authority {
    Readable(InvalidationRecord),
    Unreadable,
}

impl Authority {
    #[must_use]
    pub fn generation(self) -> Option<u64> {
        match self {
            Self::Readable(record) => Some(record.generation),
            Self::Unreadable => None,
        }
    }

    #[must_use]
    pub fn permits_checkpoint(self) -> bool {
        matches!(
            self,
            Self::Readable(InvalidationRecord { active: false, .. })
        )
    }
}

/// Read before hashing or walking. A non-NotFound I/O error is never treated as generation zero.
pub fn read_authority(source_log: &Path) -> io::Result<Authority> {
    match std::fs::read(record_path(source_log)) {
        Ok(bytes) => Ok(serde_json::from_slice(&bytes)
            .map(Authority::Readable)
            .unwrap_or(Authority::Unreadable)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            Ok(Authority::Readable(InvalidationRecord::default()))
        }
        Err(error) => Err(error),
    }
}

struct CheckpointLock {
    _file: File,
}

impl CheckpointLock {
    fn acquire(source_log: &Path) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(suffixed(&checkpoint_path(source_log), ".lock"))?;
        fs2::FileExt::lock_exclusive(&file)?;
        Ok(Self { _file: file })
    }
}

// One checksum/header/applicability check for both the full loader and publication. Only the
// loader additionally requires the exact reducer version and decodes reducer bodies.
fn check_artifact(
    path: &Path,
    activation: &LogTailBinding,
    financial_era: bool,
) -> io::Result<(ArtifactCheck, Vec<u8>)> {
    let bytes = match std::fs::read(checkpoint_path(path)) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok((ArtifactCheck::Absent, Vec::new()));
        }
        Err(error) => return Err(error),
    };
    let Some(body) = bytes.get(CHECKPOINT_HEADER_LEN..) else {
        return Ok((ArtifactCheck::Invalid, bytes));
    };
    if bytes.get(..64) != Some(blake3::hash(body).to_hex().as_bytes()) || bytes[64] != b'\n' {
        return Ok((ArtifactCheck::Invalid, bytes));
    }
    let header: HeaderView = match serde_json::from_slice(body) {
        Ok(header) => header,
        Err(_) => return Ok((ArtifactCheck::Invalid, bytes)),
    };
    let file_len = std::fs::metadata(path)?.len();
    if header.format_version != 1
        || header.scanner_version != 1
        || header.financial_era != financial_era
        || header.activation != *activation
        || header.tail.path != activation.path
        || header.tail.physical_tail < activation.physical_tail
        || header.tail.physical_tail > file_len
        || header.tail.last_sequence < activation.last_sequence
        || (header.tail.physical_tail > activation.physical_tail
            && header.tail.last_sequence == activation.last_sequence)
        || header.daily_boundary.is_some() != financial_era
        || (header.tail.physical_tail == activation.physical_tail && header.tail != *activation)
        || SourceReceiptIndex::checkpoint_metadata_valid(path, &header.receipts, &header.tail)
            .is_err()
    {
        return Ok((ArtifactCheck::Inapplicable, bytes));
    }
    Ok((ArtifactCheck::Valid(Box::new(header)), bytes))
}

pub(crate) fn load_checkpoint(
    path: &Path,
    activation: &LogTailBinding,
    financial_era: bool,
) -> Option<LoadedCheckpoint> {
    let (check, bytes) = check_artifact(path, activation, financial_era).ok()?;
    let ArtifactCheck::Valid(header) = check else {
        return None;
    };
    if !matches!(header.reducer_version, 2) && header.reducer_version != ACTIVITY_REDUCER_VERSION {
        return None;
    }
    drop(header);
    let data = serde_json::from_slice(bytes.get(CHECKPOINT_HEADER_LEN..)?).ok()?;
    let checksum = bytes.get(..64)?.try_into().ok()?;
    Some(LoadedCheckpoint { data, checksum })
}

/// A verified candidate serialized once; publication borrows it and never consumes its bytes.
/// Construct only from projections verified through `tail` under `authority_generation`.
pub struct SerializedCandidate {
    pub(crate) bytes: Vec<u8>,
    activation: LogTailBinding,
    financial_era: bool,
    tail: LogTailBinding,
    prefix_blake3: String,
    reducer_version: u32,
    authority_generation: u64,
    capture_unix_ms: u64,
}

pub(crate) fn serialize(
    data: CheckpointData,
    authority_generation: u64,
    capture_unix_ms: u64,
) -> Result<SerializedCandidate, serde_json::Error> {
    let mut encoded = vec![b'0'; CHECKPOINT_HEADER_LEN];
    encoded[64] = b'\n';
    serde_json::to_writer(&mut encoded, &data)?;
    let checksum = blake3::hash(&encoded[CHECKPOINT_HEADER_LEN..]).to_hex();
    encoded[..64].copy_from_slice(checksum.as_bytes());
    Ok(SerializedCandidate {
        bytes: encoded,
        activation: data.activation,
        financial_era: data.financial_era,
        tail: data.tail,
        prefix_blake3: data.prefix_blake3,
        reducer_version: data.reducer_version,
        authority_generation,
        capture_unix_ms,
    })
}

/// Receipt of a durably installed checkpoint, including successful active-record clearance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicationReceipt {
    pub tail: LogTailBinding,
    pub prefix_blake3: String,
    pub capture_unix_ms: u64,
    pub published_unix_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum RefusalReason {
    #[error("unreadable invalidation record")]
    UnreadableRecord,
    #[error("current checkpoint has a newer reducer version")]
    NewerReducer,
    #[error("current checkpoint has a later tail")]
    LaterTail,
    #[error("checkpoint bindings conflict at the same offset")]
    ConflictingBinding,
    #[error("checkpoint prefix digests conflict at the same binding")]
    ConflictingPrefix,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublishOutcome {
    Published(PublicationReceipt),
    Refused(RefusalReason),
    GenerationChanged { candidate: u64, current: u64 },
}

#[derive(Debug, Error)]
pub enum PublishError {
    #[error("source checkpoint publication I/O: {0}")]
    Io(#[from] io::Error),
}

#[derive(Debug, Error)]
pub enum InvalidationError {
    #[error("source checkpoint quarantine could not be made durable: {0}")]
    QuarantineFailed(#[source] io::Error),
    #[error("source checkpoint invalidation I/O: {0}")]
    Io(#[from] io::Error),
    #[error("source checkpoint authority unreadable; quiesced recovery required")]
    UnreadableRecord,
    #[error("source checkpoint generation exhausted")]
    GenerationOverflow,
}

pub(crate) fn unix_ms() -> io::Result<u64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(io::Error::other)?;
    u64::try_from(duration.as_millis()).map_err(io::Error::other)
}

// Private finalization seam: ordinary builds always use the canonical writer; unit tests inject
// failures at protocol boundaries without adding a public testing API or another atomic writer.
trait Finalization {
    fn quarantine_rename(&mut self, from: &Path, to: &Path) -> io::Result<()> {
        std::fs::rename(from, to)
    }
    fn sync_directory(&mut self, target: &Path) -> io::Result<()> {
        sync_directory(target)
    }
    fn checkpoint_write(&mut self, target: &Path, bytes: &[u8]) -> io::Result<()> {
        crate::qualification::write_report(target, bytes)
    }
    fn record_write(&mut self, target: &Path, bytes: &[u8]) -> io::Result<()> {
        crate::qualification::write_report(target, bytes)
    }
}
struct DurableFinalization;
impl Finalization for DurableFinalization {}

fn sync_directory(target: &Path) -> io::Result<()> {
    let parent = target
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    File::open(parent)?.sync_all()
}

fn remove_if_present(path: &Path) -> io::Result<bool> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

fn remove_own_temporaries(source_log: &Path) -> io::Result<()> {
    for target in [checkpoint_path(source_log), record_path(source_log)] {
        remove_if_present(&target.with_extension(format!("tmp.{}", std::process::id())))?;
    }
    Ok(())
}

/// Compare and atomically install these exact bytes. I/O failures are retryable with this same
/// candidate; generation changes are distinct from refusals that require a fresh candidate.
pub fn publish(
    candidate: &SerializedCandidate,
    attempt: u32,
) -> Result<PublishOutcome, PublishError> {
    publish_with_finalization(candidate, attempt, &mut DurableFinalization, unix_ms)
}

fn publish_with_finalization(
    candidate: &SerializedCandidate,
    attempt: u32,
    finalization: &mut impl Finalization,
    clock: impl FnOnce() -> io::Result<u64>,
) -> Result<PublishOutcome, PublishError> {
    let mut decision = "";
    let mut write_ms = 0;
    let result = (|| {
        let path = &candidate.tail.path;
        let _lock = CheckpointLock::acquire(path)?;
        remove_own_temporaries(path)?;
        let Authority::Readable(record) = read_authority(path)? else {
            return Ok(PublishOutcome::Refused(RefusalReason::UnreadableRecord));
        };
        if record.generation != candidate.authority_generation {
            return Ok(PublishOutcome::GenerationChanged {
                candidate: candidate.authority_generation,
                current: record.generation,
            });
        }
        let (check, bytes) = check_artifact(path, &candidate.activation, candidate.financial_era)?;
        drop(bytes);
        match check {
            ArtifactCheck::Absent => decision = "replace_absent",
            ArtifactCheck::Invalid => decision = "replace_invalid",
            ArtifactCheck::Inapplicable => decision = "replace_inapplicable",
            ArtifactCheck::Valid(current) => {
                if current.reducer_version > candidate.reducer_version {
                    return Ok(PublishOutcome::Refused(RefusalReason::NewerReducer));
                }
                if current.reducer_version < candidate.reducer_version {
                    decision = "replace_older_reducer";
                } else if current.tail.physical_tail > candidate.tail.physical_tail {
                    return Ok(PublishOutcome::Refused(RefusalReason::LaterTail));
                } else if current.tail.physical_tail == candidate.tail.physical_tail {
                    if current.tail != candidate.tail {
                        return Ok(PublishOutcome::Refused(RefusalReason::ConflictingBinding));
                    }
                    if current.prefix_blake3 != candidate.prefix_blake3 && !record.active {
                        return Ok(PublishOutcome::Refused(RefusalReason::ConflictingPrefix));
                    }
                    decision = if current.prefix_blake3 == candidate.prefix_blake3 {
                        "rewrite_equal"
                    } else {
                        "replace_active_prefix"
                    };
                } else {
                    decision = "replace_earlier_tail";
                }
            }
        }
        let write_started = Instant::now();
        finalization.checkpoint_write(&checkpoint_path(path), &candidate.bytes)?;
        if record.active {
            let cleared = InvalidationRecord {
                active: false,
                ..record
            };
            finalization.record_write(&record_path(path), &record_bytes(cleared)?)?;
        }
        write_ms = u64::try_from(write_started.elapsed().as_millis()).unwrap_or(u64::MAX);
        Ok(PublishOutcome::Published(PublicationReceipt {
            tail: candidate.tail.clone(),
            prefix_blake3: candidate.prefix_blake3.clone(),
            capture_unix_ms: candidate.capture_unix_ms,
            published_unix_ms: clock()?,
        }))
    })();
    match &result {
        Ok(PublishOutcome::Published(receipt)) => info!(
            offset = receipt.tail.physical_tail,
            sequence = %receipt.tail.last_sequence.map_or_else(|| "none".to_owned(), |seq| seq.0.to_string()),
            hash = %receipt.tail.last_hash.to_hex(),
            prefix_blake3 = %receipt.prefix_blake3,
            capture_unix_ms = receipt.capture_unix_ms,
            published_unix_ms = receipt.published_unix_ms,
            write_ms,
            bytes = candidate.bytes.len(), attempt, decision,
            "source checkpoint published"
        ),
        Ok(outcome) => info!(reason = ?outcome, capture_unix_ms = candidate.capture_unix_ms,
            "source checkpoint candidate refused"),
        // An I/O error leaves the candidate retryable; its caller reports the failed attempt.
        Err(_) => {}
    }
    result
}

fn record_bytes(record: InvalidationRecord) -> io::Result<Vec<u8>> {
    serde_json::to_vec(&record).map_err(io::Error::other)
}

/// Establish a durable quarantine before changing the authority generation. A quarantine failure
/// is typed separately so the runtime owner can suppress automatic restart.
pub fn invalidate(source_log: &Path) -> Result<u64, InvalidationError> {
    let _lock = CheckpointLock::acquire(source_log).map_err(InvalidationError::QuarantineFailed)?;
    invalidate_locked(source_log, &mut DurableFinalization)
}

fn invalidate_locked(
    path: &Path,
    finalization: &mut impl Finalization,
) -> Result<u64, InvalidationError> {
    let authority = read_authority(path).map_err(InvalidationError::QuarantineFailed)?;
    let checkpoint = checkpoint_path(path);
    let record = record_path(path);
    let renamed = match finalization.quarantine_rename(&checkpoint, &record) {
        Ok(()) => {
            finalization
                .sync_directory(&record)
                .map_err(InvalidationError::QuarantineFailed)?;
            true
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => false,
        Err(error) => return Err(InvalidationError::QuarantineFailed(error)),
    };
    let Authority::Readable(current) = authority else {
        return Err(InvalidationError::UnreadableRecord);
    };
    let generation = current
        .generation
        .checked_add(1)
        .ok_or(InvalidationError::GenerationOverflow)?;
    let bytes = record_bytes(InvalidationRecord {
        generation,
        active: true,
    })?;
    finalization
        .record_write(&record, &bytes)
        .map_err(|error| {
            if renamed {
                InvalidationError::Io(error)
            } else {
                InvalidationError::QuarantineFailed(error)
            }
        })?;
    Ok(generation)
}

pub(crate) enum ConditionalInvalidation {
    Invalidated(u64),
    Changed,
}

/// A preparation may invalidate only the exact artifact and authority it hashed.
pub(crate) fn invalidate_if_current(
    source_log: &Path,
    generation: u64,
    checksum: &[u8; 64],
) -> Result<ConditionalInvalidation, InvalidationError> {
    let _lock = CheckpointLock::acquire(source_log)?;
    if read_authority(source_log)?.generation() != Some(generation) {
        return Ok(ConditionalInvalidation::Changed);
    }
    let mut file = match File::open(checkpoint_path(source_log)) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(ConditionalInvalidation::Changed);
        }
        Err(error) => return Err(error.into()),
    };
    let mut current = [0; 64];
    match file.read_exact(&mut current) {
        Ok(()) if &current == checksum => {}
        Ok(()) => return Ok(ConditionalInvalidation::Changed),
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => {
            return Ok(ConditionalInvalidation::Changed);
        }
        Err(error) => return Err(error.into()),
    }
    drop(file);
    invalidate_locked(source_log, &mut DurableFinalization)
        .map(ConditionalInvalidation::Invalidated)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryReceipt {
    pub checkpoint: PathBuf,
    pub checkpoint_removed: bool,
    pub record: PathBuf,
    pub record_removed: bool,
}

/// The operator must stop and drain the service and finish or terminate every preparation first.
/// Remove and sync the checkpoint before resetting its authority; the persistent lock stays.
pub fn recover_quiesced(source_log: &Path) -> io::Result<RecoveryReceipt> {
    recover_with_finalization(source_log, &mut DurableFinalization)
}

fn recover_with_finalization(
    path: &Path,
    finalization: &mut impl Finalization,
) -> io::Result<RecoveryReceipt> {
    let _lock = CheckpointLock::acquire(path)?;
    let checkpoint = checkpoint_path(path);
    let record = record_path(path);
    let checkpoint_removed = remove_if_present(&checkpoint)?;
    finalization.sync_directory(&checkpoint)?;
    let record_removed = remove_if_present(&record)?;
    finalization.sync_directory(&record)?;
    Ok(RecoveryReceipt {
        checkpoint,
        checkpoint_removed,
        record,
        record_removed,
    })
}

pub(crate) fn installed_activation(paper_path: &Path) -> anyhow::Result<LogTailBinding> {
    use anyhow::{Context as _, ensure};
    use pe_paper_state::{MigrationMetadata, MigrationPhase};
    let record = MigrationMetadata::read_read_only(paper_path)?
        .context("checkpoint command requires installed migration metadata")?;
    ensure!(
        record.phase == MigrationPhase::Installed,
        "checkpoint command requires installed paper state"
    );
    Ok(record
        .activation_tails
        .context("installed migration omitted activation tails")?
        .source)
}

/// Stop the service and every preparation before invoking this installed-state recovery command.
/// The source-log path comes only from the installed migration record.
pub fn recover_installed(paper_path: &Path) -> anyhow::Result<RecoveryReceipt> {
    Ok(recover_quiesced(&installed_activation(paper_path)?.path)?)
}

/// Preparation pause hooks, absent from ordinary builds.
#[cfg(feature = "scenario")]
#[derive(Default)]
pub struct PreparationHooks {
    pub after_bound: Option<std::sync::Arc<dyn Fn() -> io::Result<()> + Send + Sync>>,
    pub before_cached_hash: Option<std::sync::Arc<dyn Fn() -> io::Result<()> + Send + Sync>>,
    pub after_cached_hash: Option<std::sync::Arc<dyn Fn() -> io::Result<()> + Send + Sync>>,
    pub after_walk: Option<std::sync::Arc<dyn Fn() -> io::Result<()> + Send + Sync>>,
}

/// Real-binary preparation rendezvous, present only in scenario builds. The ready/resume files
/// synchronize the fixture with the publisher after verification and before publication.
#[cfg(feature = "scenario")]
pub fn cli_preparation_hooks() -> io::Result<Option<PreparationHooks>> {
    let Some(path) = std::env::var_os("PE_SCENARIO_CHECKPOINT_PAUSE_AFTER_WALK") else {
        return Ok(None);
    };
    let path = PathBuf::from(path);
    Ok(Some(PreparationHooks {
        after_walk: Some(std::sync::Arc::new(move || {
            std::fs::write(suffixed(&path, ".ready"), b"ready")?;
            let deadline = Instant::now() + std::time::Duration::from_secs(30);
            while !suffixed(&path, ".resume").try_exists()? {
                if Instant::now() >= deadline {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "scenario preparation resume not received",
                    ));
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            Ok(())
        })),
        ..PreparationHooks::default()
    }))
}

/// Scenario-only one-shot invalidation faults; production always uses durable finalization.
#[cfg(feature = "scenario")]
#[derive(Default)]
pub struct InvalidationHooks {
    pub fail_quarantine_rename: std::sync::atomic::AtomicBool,
    pub fail_quarantine_sync: std::sync::atomic::AtomicBool,
    pub fail_record_write: std::sync::atomic::AtomicBool,
}

#[cfg(feature = "scenario")]
pub fn invalidate_with_hooks(
    path: &Path,
    hooks: &InvalidationHooks,
) -> Result<u64, InvalidationError> {
    use std::sync::atomic::Ordering;
    struct ScenarioFinalization<'a>(&'a InvalidationHooks);
    impl Finalization for ScenarioFinalization<'_> {
        fn quarantine_rename(&mut self, from: &Path, to: &Path) -> io::Result<()> {
            if self.0.fail_quarantine_rename.swap(false, Ordering::SeqCst) {
                return Err(io::Error::other("scenario quarantine rename"));
            }
            std::fs::rename(from, to)
        }
        fn sync_directory(&mut self, path: &Path) -> io::Result<()> {
            if self.0.fail_quarantine_sync.swap(false, Ordering::SeqCst) {
                return Err(io::Error::other("scenario quarantine sync"));
            }
            sync_directory(path)
        }
        fn record_write(&mut self, target: &Path, bytes: &[u8]) -> io::Result<()> {
            if self.0.fail_record_write.swap(false, Ordering::SeqCst) {
                return Err(io::Error::other("scenario record write"));
            }
            crate::qualification::write_report(target, bytes)
        }
    }
    let _lock = CheckpointLock::acquire(path).map_err(InvalidationError::QuarantineFailed)?;
    invalidate_locked(path, &mut ScenarioFinalization(hooks))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use pe_core_types::{ReceivedAt, SourceId, SourceTimestamp};
    use pe_event_log::{ContentType, EnvelopeIn, Writer};
    use std::sync::mpsc;
    use time::OffsetDateTime;

    const CAPTURE: u64 = 1_700_000_000_000;
    const PUBLISHED: u64 = CAPTURE + 25;

    struct Fixture {
        _dir: tempfile::TempDir,
        path: PathBuf,
        activation: LogTailBinding,
        tails: Vec<LogTailBinding>,
    }

    impl Fixture {
        fn new() -> Self {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("source_events.log");
            let mut writer = Writer::open(&path).unwrap();
            let activation = writer.verified_tail().unwrap();
            let mut tails = Vec::new();
            for payload in [b"first".as_slice(), b"second", b"third"] {
                let clock = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
                writer
                    .append_synced(EnvelopeIn {
                        source_id: SourceId("checkpoint-fixture".into()),
                        schema_version: 1,
                        parser_version: 1,
                        observed_at: SourceTimestamp(clock),
                        received_at: ReceivedAt(clock),
                        content_type: ContentType::Raw,
                        payload: payload.to_vec(),
                    })
                    .unwrap();
                tails.push(writer.verified_tail().unwrap());
            }
            drop(writer);
            Self {
                _dir: dir,
                path,
                activation,
                tails,
            }
        }

        fn data(&self, tail: usize, reducer_version: u32) -> CheckpointData {
            let tail = self.tails[tail].clone();
            let index = SourceReceiptIndex::replay(&self.path).unwrap();
            CheckpointData {
                format_version: 1,
                scanner_version: 1,
                reducer_version,
                financial_era: false,
                activation: self.activation.clone(),
                prefix_blake3: pe_event_log::Scanner::hash_prefix(&self.path, tail.physical_tail)
                    .unwrap()
                    .finalize()
                    .to_hex()
                    .to_string(),
                receipts: index
                    .checkpoint_prefix(
                        usize::try_from(tail.last_sequence.unwrap().0 + 1).unwrap(),
                        &tail,
                    )
                    .unwrap(),
                tail,
                activity: ActivityCandidates::default(),
                daily_boundary: None,
            }
        }

        fn candidate(&self, tail: usize, version: u32, generation: u64) -> SerializedCandidate {
            serialize(self.data(tail, version), generation, CAPTURE).unwrap()
        }

        fn stage(&self, candidate: &SerializedCandidate) {
            crate::qualification::write_report(&checkpoint_path(&self.path), &candidate.bytes)
                .unwrap();
        }

        fn record(&self, generation: u64, active: bool) {
            crate::qualification::write_report(
                &record_path(&self.path),
                &record_bytes(InvalidationRecord { generation, active }).unwrap(),
            )
            .unwrap();
        }
    }

    fn attempt(
        candidate: &SerializedCandidate,
        seam: &mut impl Finalization,
    ) -> Result<PublishOutcome, PublishError> {
        publish_with_finalization(candidate, 1, seam, || Ok(PUBLISHED))
    }

    fn receipt(outcome: PublishOutcome) -> PublicationReceipt {
        match outcome {
            PublishOutcome::Published(receipt) => receipt,
            other => panic!("expected publication, got {other:?}"),
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Fault {
        QuarantineRename,
        QuarantineSync,
        RecordBeforeWrite,
        RecordAfterRename,
        CheckpointBeforeWrite,
        CheckpointAfterRename,
    }

    struct Failure {
        fault: Option<Fault>,
        checkpoint_writes: usize,
        record_writes: usize,
    }
    impl Failure {
        fn new(fault: Fault) -> Self {
            Self {
                fault: Some(fault),
                checkpoint_writes: 0,
                record_writes: 0,
            }
        }
        fn fails(&mut self, boundary: Fault) -> bool {
            if self.fault == Some(boundary) {
                self.fault = None;
                true
            } else {
                false
            }
        }
    }

    fn failed_sync(path: &Path, bytes: &[u8]) -> io::Result<()> {
        crate::qualification::write_report_with_finalization(
            path,
            bytes,
            |from, to| std::fs::rename(from, to),
            |_| Err(io::Error::other("injected directory sync failure")),
        )
    }

    impl Finalization for Failure {
        fn quarantine_rename(&mut self, from: &Path, to: &Path) -> io::Result<()> {
            if self.fails(Fault::QuarantineRename) {
                return Err(io::Error::other("injected quarantine rename"));
            }
            std::fs::rename(from, to)
        }
        fn sync_directory(&mut self, target: &Path) -> io::Result<()> {
            if self.fails(Fault::QuarantineSync) {
                return Err(io::Error::other("injected quarantine directory sync"));
            }
            sync_directory(target)
        }
        fn checkpoint_write(&mut self, target: &Path, bytes: &[u8]) -> io::Result<()> {
            self.checkpoint_writes += 1;
            if self.fails(Fault::CheckpointBeforeWrite) {
                return Err(io::Error::other("injected checkpoint write"));
            }
            if self.fails(Fault::CheckpointAfterRename) {
                return failed_sync(target, bytes);
            }
            crate::qualification::write_report(target, bytes)
        }
        fn record_write(&mut self, target: &Path, bytes: &[u8]) -> io::Result<()> {
            self.record_writes += 1;
            if self.fails(Fault::RecordBeforeWrite) {
                return Err(io::Error::other("injected record write"));
            }
            if self.fails(Fault::RecordAfterRename) {
                return failed_sync(target, bytes);
            }
            crate::qualification::write_report(target, bytes)
        }
    }

    #[test]
    fn publisher_protocol_comparison() {
        for (current_version, current_tail, candidate_version, candidate_tail, refused) in [
            (1, 1, 2, 1, None), // newer-version equal-tail staging replaces
            (3, 1, 2, 1, Some(RefusalReason::NewerReducer)),
            (2, 1, 1, 2, Some(RefusalReason::NewerReducer)),
            (2, 2, 2, 1, Some(RefusalReason::LaterTail)),
            (2, 1, 2, 1, None), // equal candidate is rewritten
            (2, 1, 2, 2, None),
            (1, 2, 2, 1, None), // version protection precedes tail comparison
        ] {
            let fixture = Fixture::new();
            let current = fixture.candidate(current_tail, current_version, 0);
            fixture.stage(&current);
            let candidate = fixture.candidate(candidate_tail, candidate_version, 0);
            let result = attempt(&candidate, &mut DurableFinalization).unwrap();
            if let Some(reason) = refused {
                assert_eq!(result, PublishOutcome::Refused(reason));
                assert_eq!(
                    std::fs::read(checkpoint_path(&fixture.path)).unwrap(),
                    current.bytes
                );
            } else {
                let published = receipt(result);
                assert_eq!(published.tail, candidate.tail);
                assert_eq!(published.capture_unix_ms, CAPTURE);
                assert_eq!(published.published_unix_ms, PUBLISHED);
                assert_eq!(
                    std::fs::read(checkpoint_path(&fixture.path)).unwrap(),
                    candidate.bytes
                );
            }
        }
    }

    #[test]
    fn publisher_protocol_conflicting_equal_offset_binding_refused() {
        for conflict in ["sequence", "hash", "prefix"] {
            let fixture = Fixture::new();
            let current = fixture.candidate(1, 2, 0);
            fixture.stage(&current);
            let mut data = fixture.data(1, 2);
            match conflict {
                "sequence" => data.tail.last_sequence = Some(pe_core_types::EventSeq(99)),
                "hash" => data.tail.last_hash = blake3::hash(b"different binding"),
                "prefix" => {
                    data.prefix_blake3 = blake3::hash(b"different prefix").to_hex().to_string()
                }
                _ => unreachable!(),
            }
            let candidate = serialize(data, 0, CAPTURE).unwrap();
            assert_eq!(
                attempt(&candidate, &mut DurableFinalization).unwrap(),
                PublishOutcome::Refused(if conflict == "prefix" {
                    RefusalReason::ConflictingPrefix
                } else {
                    RefusalReason::ConflictingBinding
                })
            );
            assert_eq!(
                std::fs::read(checkpoint_path(&fixture.path)).unwrap(),
                current.bytes
            );
        }
    }

    #[test]
    fn absent_invalid_and_inapplicable_artifacts_replaced() {
        for fault in [
            "absent",
            "checksum",
            "decode",
            "format",
            "scanner",
            "mode",
            "activation",
            "path",
            "short_tail",
            "long_tail",
            "receipt",
            "boundary",
        ] {
            let fixture = Fixture::new();
            let mut data = fixture.data(1, 99); // an inapplicable newer reducer protects nothing
            match fault {
                "format" => data.format_version = 99,
                "scanner" => data.scanner_version = 99,
                "mode" => data.financial_era = true,
                "activation" => data.activation = fixture.tails[0].clone(),
                "path" => data.tail.path = fixture.path.with_extension("other"),
                "short_tail" => data.tail.physical_tail = 0,
                "long_tail" => {
                    data.tail.physical_tail = std::fs::metadata(&fixture.path).unwrap().len() + 1
                }
                "receipt" => data.receipts.clear(),
                "boundary" => data.daily_boundary = Some(DailyBoundaryCandidates::default()),
                _ => {}
            }
            let mut current = serialize(data, 0, CAPTURE).unwrap();
            if fault == "checksum" {
                current.bytes[0] ^= 1;
            }
            if fault == "decode" {
                current.bytes.truncate(CHECKPOINT_HEADER_LEN);
                current.bytes.extend_from_slice(b"invalid-json");
                let hash = blake3::hash(&current.bytes[CHECKPOINT_HEADER_LEN..]);
                current.bytes[..64].copy_from_slice(hash.to_hex().as_bytes());
            }
            if fault != "absent" {
                fixture.stage(&current);
            }
            let candidate = fixture.candidate(1, 2, 0);
            receipt(attempt(&candidate, &mut DurableFinalization).unwrap());
            assert_eq!(
                std::fs::read(checkpoint_path(&fixture.path)).unwrap(),
                candidate.bytes,
                "{fault}"
            );
            assert!(load_checkpoint(&fixture.path, &fixture.activation, false).is_some());
        }
    }

    #[test]
    fn publisher_protocol_two_publishers_racing_on_the_lock() {
        for newer_first in [false, true] {
            let fixture = Fixture::new();
            let early = fixture.candidate(0, 2, 0);
            let later = fixture.candidate(2, 2, 0);
            let (first, second) = if newer_first {
                (&later, &early)
            } else {
                (&early, &later)
            };
            let (held_tx, held_rx) = mpsc::channel();
            let (release_tx, release_rx) = mpsc::channel();
            struct PausedWrite {
                held: mpsc::Sender<()>,
                release: mpsc::Receiver<()>,
            }
            impl Finalization for PausedWrite {
                fn checkpoint_write(&mut self, path: &Path, bytes: &[u8]) -> io::Result<()> {
                    self.held.send(()).map_err(io::Error::other)?;
                    self.release.recv().map_err(io::Error::other)?;
                    crate::qualification::write_report(path, bytes)
                }
            }
            std::thread::scope(|scope| {
                let first_job = scope.spawn(move || {
                    attempt(
                        first,
                        &mut PausedWrite {
                            held: held_tx,
                            release: release_rx,
                        },
                    )
                });
                held_rx.recv().unwrap();
                let lock_file =
                    File::open(suffixed(&checkpoint_path(&fixture.path), ".lock")).unwrap();
                assert_eq!(
                    fs2::FileExt::try_lock_exclusive(&lock_file)
                        .unwrap_err()
                        .kind(),
                    io::ErrorKind::WouldBlock
                );
                let second_job = scope.spawn(|| attempt(second, &mut DurableFinalization));
                release_tx.send(()).unwrap();
                receipt(first_job.join().unwrap().unwrap());
                let result = second_job.join().unwrap().unwrap();
                if newer_first {
                    assert_eq!(result, PublishOutcome::Refused(RefusalReason::LaterTail));
                } else {
                    receipt(result);
                }
            });
            assert_eq!(
                std::fs::read(checkpoint_path(&fixture.path)).unwrap(),
                later.bytes
            );
            assert!(suffixed(&checkpoint_path(&fixture.path), ".lock").exists());
        }
    }

    #[test]
    fn equal_candidate_rewritten_despite_different_serialized_content() {
        let fixture = Fixture::new();
        let candidate = fixture.candidate(1, 2, 0);
        let mut current = fixture.candidate(1, 2, 0);
        current.bytes.push(b'\n');
        let checksum = blake3::hash(&current.bytes[CHECKPOINT_HEADER_LEN..]);
        current.bytes[..64].copy_from_slice(checksum.to_hex().as_bytes());
        assert_ne!(candidate.bytes, current.bytes);
        fixture.stage(&current);
        // This seam succeeds, and its counter proves the equal candidate took the writer path.
        let mut seam = Failure {
            fault: None,
            checkpoint_writes: 0,
            record_writes: 0,
        };
        receipt(attempt(&candidate, &mut seam).unwrap());
        assert_eq!(seam.checkpoint_writes, 1);
        assert_eq!(
            std::fs::read(checkpoint_path(&fixture.path)).unwrap(),
            candidate.bytes
        );
    }

    #[test]
    fn pre_invalidation_publisher_refused_after_clearance() {
        let fixture = Fixture::new();
        let mut stale = fixture.candidate(1, 2, 0);
        // A valid, differently encoded body with the same semantic projections and raw prefix.
        stale.bytes.push(b'\n');
        let checksum = blake3::hash(&stale.bytes[CHECKPOINT_HEADER_LEN..]);
        stale.bytes[..64].copy_from_slice(checksum.to_hex().as_bytes());
        let before = fixture.candidate(1, 2, 0);
        fixture.stage(&before);
        let generation = invalidate(&fixture.path).unwrap();
        assert_eq!(generation, 1);
        let current = fixture.candidate(1, 2, generation);
        receipt(attempt(&current, &mut DurableFinalization).unwrap());
        assert_eq!(
            read_authority(&fixture.path).unwrap(),
            Authority::Readable(InvalidationRecord {
                generation,
                active: false
            })
        );
        assert_eq!(
            attempt(&stale, &mut DurableFinalization).unwrap(),
            PublishOutcome::GenerationChanged {
                candidate: 0,
                current: 1
            }
        );
        assert_eq!(
            std::fs::read(checkpoint_path(&fixture.path)).unwrap(),
            current.bytes
        );
    }

    #[test]
    fn candidate_generation_predates_record_refused() {
        for active in [false, true] {
            let fixture = Fixture::new();
            fixture.record(3, active);
            let candidate = fixture.candidate(1, 2, 2);
            assert_eq!(
                attempt(&candidate, &mut DurableFinalization).unwrap(),
                PublishOutcome::GenerationChanged {
                    candidate: 2,
                    current: 3
                }
            );
            assert!(!checkpoint_path(&fixture.path).exists());
        }
    }

    #[test]
    fn interrupted_invalidation_at_each_durable_boundary() {
        for previous in [None, Some(false), Some(true)] {
            for present in [false, true] {
                for fault in [
                    Fault::QuarantineRename,
                    Fault::QuarantineSync,
                    Fault::RecordBeforeWrite,
                    Fault::RecordAfterRename,
                ] {
                    let fixture = Fixture::new();
                    if let Some(active) = previous {
                        fixture.record(7, active);
                    }
                    let candidate = fixture.candidate(1, 2, previous.map_or(0, |_| 7));
                    if present {
                        fixture.stage(&candidate);
                    }
                    let mut seam = Failure::new(fault);
                    let _lock = CheckpointLock::acquire(&fixture.path).unwrap();
                    let result = invalidate_locked(&fixture.path, &mut seam);
                    drop(_lock);
                    if fault == Fault::QuarantineRename
                        || present && fault == Fault::QuarantineSync
                        || !present
                            && matches!(fault, Fault::RecordBeforeWrite | Fault::RecordAfterRename)
                    {
                        assert!(
                            matches!(result, Err(InvalidationError::QuarantineFailed(_))),
                            "{present} {fault:?}"
                        );
                    } else if present
                        && matches!(fault, Fault::RecordBeforeWrite | Fault::RecordAfterRename)
                    {
                        assert!(matches!(result, Err(InvalidationError::Io(_))));
                    } else {
                        assert!(result.is_ok());
                    }
                    if present && matches!(fault, Fault::QuarantineSync | Fault::RecordBeforeWrite)
                    {
                        assert_eq!(
                            read_authority(&fixture.path).unwrap(),
                            Authority::Unreadable
                        );
                        assert_eq!(
                            attempt(&candidate, &mut DurableFinalization).unwrap(),
                            PublishOutcome::Refused(RefusalReason::UnreadableRecord)
                        );
                        assert!(!checkpoint_path(&fixture.path).exists());
                    }
                }
            }
        }
    }

    #[test]
    fn invalidation_lock_failure_cannot_establish_quarantine() {
        let fixture = Fixture::new();
        let candidate = fixture.candidate(1, 2, 0);
        fixture.stage(&candidate);
        std::fs::create_dir(suffixed(&checkpoint_path(&fixture.path), ".lock")).unwrap();
        assert!(matches!(
            invalidate(&fixture.path),
            Err(InvalidationError::QuarantineFailed(_))
        ));
        #[cfg(feature = "scenario")]
        assert!(matches!(
            invalidate_with_hooks(&fixture.path, &InvalidationHooks::default()),
            Err(InvalidationError::QuarantineFailed(_))
        ));
        assert_eq!(
            std::fs::read(checkpoint_path(&fixture.path)).unwrap(),
            candidate.bytes
        );
        // The preparation CLI still reports its own precondition failures as ordinary I/O.
        assert!(matches!(
            invalidate_if_current(&fixture.path, 0, candidate.bytes[..64].try_into().unwrap()),
            Err(InvalidationError::Io(_))
        ));
    }

    #[test]
    fn orphan_temporary_file_does_not_block_publication() {
        let fixture = Fixture::new();
        fixture.record(4, true);
        let candidate = fixture.candidate(1, 2, 4);
        let other_pid = u64::from(std::process::id()) + 1;
        let mut kept = Vec::new();
        for target in [checkpoint_path(&fixture.path), record_path(&fixture.path)] {
            std::fs::write(
                target.with_extension(format!("tmp.{}", std::process::id())),
                b"orphan",
            )
            .unwrap();
            let other = target.with_extension(format!("tmp.{other_pid}"));
            std::fs::write(&other, b"other process").unwrap();
            kept.push(other);
        }
        assert_eq!(
            checkpoint_path(&fixture.path).with_extension(format!("tmp.{}", std::process::id())),
            fixture
                .path
                .with_extension(format!("log.tmp.{}", std::process::id()))
        );
        let published = receipt(attempt(&candidate, &mut DurableFinalization).unwrap());
        assert_eq!(published.capture_unix_ms, CAPTURE);
        assert_eq!(
            std::fs::read(checkpoint_path(&fixture.path)).unwrap(),
            candidate.bytes
        );
        for path in kept {
            assert_eq!(std::fs::read(path).unwrap(), b"other process");
        }
        assert_eq!(
            read_authority(&fixture.path).unwrap(),
            Authority::Readable(InvalidationRecord {
                generation: 4,
                active: false
            })
        );
    }

    #[test]
    fn equal_candidate_rewrite_completes_durability() {
        for active in [false, true] {
            for fault in [
                Fault::CheckpointBeforeWrite,
                Fault::CheckpointAfterRename,
                Fault::RecordBeforeWrite,
                Fault::RecordAfterRename,
            ] {
                if !active && matches!(fault, Fault::RecordBeforeWrite | Fault::RecordAfterRename) {
                    continue;
                }
                let fixture = Fixture::new();
                fixture.record(2, active);
                let candidate = fixture.candidate(1, 2, 2);
                let bytes = candidate.bytes.clone();
                let mut seam = Failure::new(fault);
                assert!(matches!(
                    attempt(&candidate, &mut seam),
                    Err(PublishError::Io(_))
                ));
                assert_eq!(candidate.bytes, bytes); // neither failure consumes nor reserializes it
                let published = receipt(attempt(&candidate, &mut seam).unwrap());
                assert_eq!(seam.checkpoint_writes, 2); // equal visible artifact is rewritten
                assert_eq!(published.capture_unix_ms, CAPTURE);
                assert_eq!(published.published_unix_ms, PUBLISHED);
                assert_eq!(
                    std::fs::read(checkpoint_path(&fixture.path)).unwrap(),
                    bytes
                );
                assert_eq!(
                    read_authority(&fixture.path).unwrap(),
                    Authority::Readable(InvalidationRecord {
                        generation: 2,
                        active: false
                    })
                );
            }
        }
    }

    #[test]
    fn active_equal_binding_replaces_wrong_prefix_and_clears() {
        let fixture = Fixture::new();
        let mut data = fixture.data(1, 2);
        data.prefix_blake3 = blake3::hash(b"wrong").to_hex().to_string();
        let wrong = serialize(data, 1, CAPTURE).unwrap();
        fixture.stage(&wrong);
        fixture.record(1, true);
        let candidate = fixture.candidate(1, 2, 1);
        receipt(attempt(&candidate, &mut DurableFinalization).unwrap());
        assert_eq!(
            std::fs::read(checkpoint_path(&fixture.path)).unwrap(),
            candidate.bytes
        );
        assert_eq!(
            read_authority(&fixture.path).unwrap(),
            Authority::Readable(InvalidationRecord {
                generation: 1,
                active: false
            })
        );
    }

    #[test]
    fn unreadable_invalidation_quarantines_surviving_checkpoint() {
        for present in [false, true] {
            let fixture = Fixture::new();
            let candidate = fixture.candidate(1, 2, 0);
            if present {
                fixture.stage(&candidate);
            }
            std::fs::write(record_path(&fixture.path), b"unreadable").unwrap();
            assert!(matches!(
                invalidate(&fixture.path),
                Err(InvalidationError::UnreadableRecord)
            ));
            assert!(!checkpoint_path(&fixture.path).exists());
            assert_eq!(
                read_authority(&fixture.path).unwrap(),
                Authority::Unreadable
            );
            assert_eq!(
                attempt(&candidate, &mut DurableFinalization).unwrap(),
                PublishOutcome::Refused(RefusalReason::UnreadableRecord)
            );
        }
    }

    #[test]
    fn authority_read_errors_are_not_generation_zero() {
        let fixture = Fixture::new();
        let candidate = fixture.candidate(1, 2, 0);
        fixture.stage(&candidate);
        std::fs::create_dir(record_path(&fixture.path)).unwrap();
        assert!(read_authority(&fixture.path).is_err());
        assert!(matches!(
            attempt(&candidate, &mut DurableFinalization),
            Err(PublishError::Io(_))
        ));
        assert!(matches!(
            invalidate(&fixture.path),
            Err(InvalidationError::QuarantineFailed(_))
        ));
        #[cfg(feature = "scenario")]
        assert!(matches!(
            invalidate_with_hooks(&fixture.path, &InvalidationHooks::default()),
            Err(InvalidationError::QuarantineFailed(_))
        ));
        assert!(matches!(
            invalidate_if_current(&fixture.path, 0, candidate.bytes[..64].try_into().unwrap()),
            Err(InvalidationError::Io(_))
        ));
        assert_eq!(
            std::fs::read(checkpoint_path(&fixture.path)).unwrap(),
            candidate.bytes
        );
    }

    #[test]
    fn quiesced_recovery_orders_removal_and_sync() {
        for fail_at in [0, 1, 2] {
            let fixture = Fixture::new();
            let candidate = fixture.candidate(1, 2, 0);
            fixture.stage(&candidate);
            std::fs::write(record_path(&fixture.path), b"quarantine").unwrap();
            struct OrderedRecovery {
                checkpoint: PathBuf,
                record: PathBuf,
                calls: usize,
                fail_at: usize,
            }
            impl Finalization for OrderedRecovery {
                fn sync_directory(&mut self, target: &Path) -> io::Result<()> {
                    self.calls += 1;
                    assert!(!self.checkpoint.exists());
                    if self.calls == 1 {
                        assert_eq!(target, self.checkpoint);
                        assert!(self.record.exists());
                    } else {
                        assert_eq!(target, self.record);
                        assert!(!self.record.exists());
                    }
                    if self.calls == self.fail_at {
                        return Err(io::Error::other("recovery directory sync"));
                    }
                    sync_directory(target)
                }
            }
            let mut seam = OrderedRecovery {
                checkpoint: checkpoint_path(&fixture.path),
                record: record_path(&fixture.path),
                calls: 0,
                fail_at,
            };
            assert_eq!(
                recover_with_finalization(&fixture.path, &mut seam).is_ok(),
                fail_at == 0
            );
            assert_eq!(seam.calls, if fail_at == 1 { 1 } else { 2 });
            assert_eq!(record_path(&fixture.path).exists(), fail_at == 1);
            recover_quiesced(&fixture.path).unwrap();
        }
    }

    #[test]
    fn quiesced_recovery_procedure() {
        for fault in [None, Some(Fault::QuarantineSync)] {
            let fixture = Fixture::new();
            let candidate = fixture.candidate(1, 2, 0);
            fixture.stage(&candidate);
            std::fs::write(record_path(&fixture.path), b"quarantine").unwrap();
            if let Some(fault) = fault {
                let result = recover_with_finalization(&fixture.path, &mut Failure::new(fault));
                assert!(result.is_err());
                assert!(!checkpoint_path(&fixture.path).exists());
                assert!(record_path(&fixture.path).exists());
            }
            let receipt = recover_quiesced(&fixture.path).unwrap();
            assert_eq!(receipt.checkpoint_removed, fault.is_none());
            assert!(receipt.record_removed);
            assert!(!checkpoint_path(&fixture.path).exists());
            assert_eq!(
                read_authority(&fixture.path).unwrap(),
                Authority::Readable(InvalidationRecord::default())
            );
            assert!(suffixed(&checkpoint_path(&fixture.path), ".lock").exists());
            super::tests::receipt(attempt(&candidate, &mut DurableFinalization).unwrap());
        }
    }
}
