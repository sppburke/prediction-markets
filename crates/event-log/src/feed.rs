//! Verbatim feed frames committed by the retention authority, without another wire format.

use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use blake3::Hash;
use pe_core_types::EventSeq;

use crate::frame::{read_frame_observed, verify_file_header};
use crate::retention::{suffixed, sync_parent};
use crate::scanner::{ScanState, ScanStep, read_verified_frame_observed, verify_envelope};
use crate::{
    EventEnvelope, FeedEntry, LogError, RECEIPT_RECORD_LEN, ReceiptRecord, RetentionAuthority,
};

pub fn feed_directory(log: &Path) -> PathBuf {
    suffixed(log, ".feed")
}

pub fn feed_path(log: &Path, epoch: u64) -> PathBuf {
    feed_directory(log).join(format!("{epoch}.frames"))
}

fn invalid(epoch: u64, message: impl Into<String>) -> LogError {
    LogError::FeedArchive {
        epoch,
        message: message.into(),
    }
}

/// The caller holds the retention publication lock; committed files are never rewritten.
pub struct FeedArchiveWriter {
    source: BufReader<File>,
    output: File,
    temporary: PathBuf,
    destination: PathBuf,
    epoch: u64,
    count: u64,
    length: u64,
    digest: blake3::Hasher,
    previous: Option<EventSeq>,
    failed: bool,
}

impl FeedArchiveWriter {
    pub fn new(log: &Path, epoch: u64) -> Result<Self, LogError> {
        let committed = RetentionAuthority::load(log)?.map_or(0, |authority| authority.epoch);
        if epoch <= committed {
            return Err(invalid(epoch, "epoch is already committed"));
        }
        let mut source = BufReader::new(File::open(log)?);
        verify_file_header(log, &mut source)?;
        let directory = feed_directory(log);
        std::fs::create_dir_all(&directory)?;
        sync_parent(&directory)?;
        let destination = feed_path(log, epoch);
        let temporary = suffixed(&destination, ".tmp");
        let output = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary)?;
        Ok(Self {
            source,
            output,
            temporary,
            destination,
            epoch,
            count: 0,
            length: 0,
            digest: blake3::Hasher::new(),
            previous: None,
            failed: false,
        })
    }

    /// Copy a frame only after the scanner verifies it; gaps between feed sequences are allowed.
    pub fn append(
        &mut self,
        offset: u64,
        sequence: EventSeq,
        predecessor_hash: Hash,
    ) -> Result<(), LogError> {
        if self.failed {
            return Err(invalid(self.epoch, "feed write previously failed"));
        }
        if self.previous.is_some_and(|previous| sequence <= previous) {
            return Err(invalid(self.epoch, "feed sequences must increase"));
        }
        self.source.seek(SeekFrom::Start(offset))?;
        let mut state = ScanState::at_frame(sequence, predecessor_hash, offset);
        let mut bytes = Vec::new();
        match read_verified_frame_observed(&mut self.source, &mut state, &mut |raw| {
            bytes.extend_from_slice(raw)
        })? {
            ScanStep::Frame(_) => {}
            _ => return Err(invalid(self.epoch, "source feed frame is incomplete")),
        }
        let length = u64::try_from(bytes.len()).map_err(std::io::Error::other)?;
        let next_length = self
            .length
            .checked_add(length)
            .ok_or(LogError::SequenceOverflow)?;
        let next_count = self
            .count
            .checked_add(1)
            .ok_or(LogError::SequenceOverflow)?;
        if let Err(error) = self.output.write_all(&bytes) {
            self.failed = true;
            return Err(error.into());
        }
        self.digest.update(&bytes);
        self.length = next_length;
        self.count = next_count;
        self.previous = Some(sequence);
        Ok(())
    }

    pub fn finish(self) -> Result<Option<FeedEntry>, LogError> {
        if self.failed {
            return Err(invalid(self.epoch, "feed write previously failed"));
        }
        if self.count == 0 {
            std::fs::remove_file(&self.temporary)?;
            match std::fs::remove_file(&self.destination) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            sync_parent(&self.destination)?;
            return Ok(None);
        }
        self.output.sync_all()?;
        std::fs::rename(&self.temporary, &self.destination)?;
        sync_parent(&self.destination)?;
        Ok(Some(FeedEntry {
            epoch: self.epoch,
            frame_count: self.count,
            length: self.length,
            hash: self.digest.finalize(),
        }))
    }
}

/// Each epoch is fully authenticated before yielding any of its frames. Unlisted files are ignored.
pub struct FeedArchiveIter {
    log: PathBuf,
    entries: std::vec::IntoIter<FeedEntry>,
    receipts: BufReader<File>,
    current: Option<(FeedEntry, BufReader<File>)>,
    previous: Option<EventSeq>,
    poisoned: bool,
}

impl FeedArchiveIter {
    pub fn open(
        log: &Path,
        authority: &RetentionAuthority,
        receipts: &Path,
    ) -> Result<Self, LogError> {
        authority.validate()?;
        Ok(Self {
            log: log.to_owned(),
            entries: authority.feed.clone().into_iter(),
            receipts: BufReader::new(File::open(receipts)?),
            current: None,
            previous: None,
            poisoned: false,
        })
    }

    fn open_epoch(&mut self, entry: &FeedEntry) -> Result<BufReader<File>, LogError> {
        let file = File::open(feed_path(&self.log, entry.epoch))
            .map_err(|error| invalid(entry.epoch, error.to_string()))?;
        if file.metadata()?.len() != entry.length {
            return Err(invalid(entry.epoch, "length mismatch"));
        }
        let mut reader = BufReader::new(file);
        let mut digest = blake3::Hasher::new();
        let mut count = 0u64;
        let mut previous = self.previous;
        while let Some(envelope) =
            archive_frame(&mut reader, &mut self.receipts, entry.epoch, &mut |bytes| {
                digest.update(bytes);
            })?
        {
            if previous.is_some_and(|seq| envelope.seq <= seq) {
                return Err(invalid(entry.epoch, "feed sequences must increase"));
            }
            previous = Some(envelope.seq);
            count = count.checked_add(1).ok_or(LogError::SequenceOverflow)?;
        }
        if count != entry.frame_count || digest.finalize() != entry.hash {
            return Err(invalid(entry.epoch, "count or hash mismatch"));
        }
        reader.seek(SeekFrom::Start(0))?;
        Ok(reader)
    }

    fn read_next(&mut self) -> Result<Option<EventEnvelope>, LogError> {
        loop {
            if let Some((entry, reader)) = &mut self.current {
                if let Some(envelope) =
                    archive_frame(reader, &mut self.receipts, entry.epoch, &mut |_| {})?
                {
                    if self.previous.is_some_and(|seq| envelope.seq <= seq) {
                        return Err(invalid(entry.epoch, "feed sequences must increase"));
                    }
                    self.previous = Some(envelope.seq);
                    return Ok(Some(envelope));
                }
                self.current = None;
            }
            let Some(entry) = self.entries.next() else {
                return Ok(None);
            };
            let reader = self.open_epoch(&entry)?;
            self.current = Some((entry, reader));
        }
    }
}

impl Iterator for FeedArchiveIter {
    type Item = Result<EventEnvelope, LogError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.poisoned {
            return None;
        }
        match self.read_next() {
            Ok(Some(envelope)) => Some(Ok(envelope)),
            Ok(None) => None,
            Err(error) => {
                self.poisoned = true;
                Some(Err(error))
            }
        }
    }
}

fn record_at(
    receipts: &mut (impl Read + Seek),
    sequence: EventSeq,
) -> Result<ReceiptRecord, LogError> {
    let offset = sequence
        .0
        .checked_mul(RECEIPT_RECORD_LEN)
        .ok_or(LogError::SequenceOverflow)?;
    receipts.seek(SeekFrom::Start(offset))?;
    Ok(ReceiptRecord::read(receipts, sequence)?)
}

fn archive_frame(
    reader: &mut (impl Read + Seek),
    receipts: &mut (impl Read + Seek),
    epoch: u64,
    raw: &mut dyn FnMut(&[u8]),
) -> Result<Option<EventEnvelope>, LogError> {
    let offset = reader.stream_position()?;
    let Some(json) = read_frame_observed(reader, offset, raw)
        .map_err(|error| invalid(epoch, format!("invalid frame: {error:?}")))?
    else {
        return Ok(None);
    };
    let envelope: EventEnvelope = serde_json::from_slice(&json)?;
    let record = record_at(receipts, envelope.seq)?;
    let predecessor = match envelope.seq.0.checked_sub(1) {
        Some(sequence) => record_at(receipts, EventSeq(sequence))?.receipt.this_hash,
        None => Hash::from_bytes([0; 32]),
    };
    verify_envelope(&envelope, record.receipt.sequence, predecessor, offset)?;
    let millis = i64::try_from(envelope.received_at.0.unix_timestamp_nanos() / 1_000_000)
        .map_err(std::io::Error::other)?;
    if envelope.this_hash != record.receipt.this_hash || millis != record.received_millis {
        return Err(invalid(epoch, "receipt mismatch"));
    }
    Ok(Some(envelope))
}
