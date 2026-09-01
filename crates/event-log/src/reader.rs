use std::fs::File;
use std::io::{BufReader, Seek, SeekFrom};
use std::path::Path;
use std::time::{Duration, Instant};

use pe_core_types::EventSeq;

use crate::frame::verify_file_header;
use crate::scanner::{LogTailBinding, ScanState, ScanStep, Scanner, read_verified_frame};
use crate::{EventEnvelope, LogError};

/// How long tail mode retries a partial frame before giving up.
const TRUNCATION_TIMEOUT: Duration = Duration::from_secs(30);

/// Read-only access to an event log file.
pub struct Reader;

impl Reader {
    /// Open `path` and return an iterator over scanner-verified frames through current EOF.
    #[tracing::instrument(skip_all, fields(path = %path.as_ref().display()))]
    pub fn replay(
        path: impl AsRef<Path>,
    ) -> Result<impl Iterator<Item = Result<(EventSeq, EventEnvelope), LogError>>, LogError> {
        // Verify through physical EOF before exposing frame 0. This prevents a consumer from
        // mutating replay state from a valid prefix before discovering corrupt interior bytes.
        Scanner::verify(path.as_ref())?;
        ReplayIter::open(path.as_ref(), None)
    }

    /// Verify the complete file and report its exact resolved-path/physical-tail binding.
    pub fn verified_tail(path: impl AsRef<Path>) -> Result<LogTailBinding, LogError> {
        Scanner::verify(path)
    }

    /// Open `path` and return an iterator that waits at EOF for newly appended frames.
    #[tracing::instrument(skip_all, fields(path = %path.as_ref().display()))]
    pub fn tail(
        path: impl AsRef<Path>,
        poll_interval: Duration,
    ) -> Result<impl Iterator<Item = Result<(EventSeq, EventEnvelope), LogError>>, LogError> {
        ReplayIter::open(path.as_ref(), Some(poll_interval))
    }
}

struct ReplayIter {
    reader: BufReader<File>,
    state: ScanState,
    poll_interval: Option<Duration>,
    poisoned: bool,
    truncation_deadline: Option<Instant>,
}

impl ReplayIter {
    fn open(path: &Path, poll_interval: Option<Duration>) -> Result<Self, LogError> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        verify_file_header(path, &mut reader)?;
        Ok(Self {
            reader,
            state: ScanState::after_header(),
            poll_interval,
            poisoned: false,
            truncation_deadline: None,
        })
    }
}

impl Iterator for ReplayIter {
    type Item = Result<(EventSeq, EventEnvelope), LogError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.poisoned {
            return None;
        }

        loop {
            let frame_start = self.state.physical_tail();
            match read_verified_frame(&mut self.reader, &mut self.state) {
                Ok(ScanStep::Frame(envelope)) => {
                    self.truncation_deadline = None;
                    return Some(Ok((envelope.seq, envelope)));
                }
                Ok(ScanStep::Eof) => match self.poll_interval {
                    None => return None,
                    Some(interval) => std::thread::sleep(interval),
                },
                Ok(ScanStep::Incomplete(incomplete)) => {
                    if let Some(interval) = self.poll_interval {
                        let deadline = self
                            .truncation_deadline
                            .get_or_insert_with(|| Instant::now() + TRUNCATION_TIMEOUT);
                        if Instant::now() < *deadline
                            && self.reader.seek(SeekFrom::Start(frame_start)).is_ok()
                        {
                            std::thread::sleep(interval);
                            continue;
                        }
                        self.truncation_deadline = None;
                    }
                    self.poisoned = true;
                    return Some(Err(LogError::Truncated {
                        at: incomplete.next_sequence,
                        byte_offset: incomplete.byte_offset,
                    }));
                }
                Err(error) => {
                    self.poisoned = true;
                    return Some(Err(error));
                }
            }
        }
    }
}
