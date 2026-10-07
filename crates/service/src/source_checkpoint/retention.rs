//! Daily source retention, serialized with live dispatch by the orchestrator.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Context as _, ensure};
use pe_core_types::{EventSeq, WalletAddress};
use pe_event_log::{
    AppendReceipt, EventEnvelope, FeedArchiveWriter, LogTailBinding, Reader, RetentionAuthority,
    RetentionBoundary, RetentionPin, Scanner, TailBinding,
};
use pe_paper_state::{DecisionPendingState, PaperStateDb};
use serde::Deserialize;
use tokio::sync::{mpsc, oneshot};
use tracing::info;

use super::{Authority, CheckpointLock};
use crate::activity_ingest::ACTIVITY_WS_SOURCE_ID;
use crate::decision_replay::{
    commitment_source_receipts, decision_source_inputs, economic_source_receipts,
};
use crate::orchestrator_control::OrchestratorControl;
use crate::paper_recovery::{FinancialPayload, PaperLog, PaperLogFrame, PaperLogRecord, paper_era};
use crate::risk_inputs::{SourceFrameMetadata, SourceReceiptIndex};
use crate::source_log_boot::{FrozenCheckpoint, FrozenPrefix};
use crate::trade_poller::PendingBoundary;

pub const SOURCE_RETENTION_BUFFER_SECS: i64 = 7 * 24 * 60 * 60;
pub const SOURCE_RETENTION_ADVANCE_SECS: i64 = 24 * 60 * 60;
const PUNCH_BLOCK: u64 = 4096;

/// Read-only inputs owned by paper-state. The adapter must include every dispatch seed, including
/// terminal rows, and every installed identity page, regardless of its decision's age.
#[derive(Default)]
pub struct RetentionDatabaseInputs {
    pub identity_sequences: Vec<EventSeq>,
    pub dispatch_seeds_exist: bool,
}

type DatabaseInputs = Arc<dyn Fn() -> anyhow::Result<RetentionDatabaseInputs> + Send + Sync>;
type InstallBoundary = Arc<dyn Fn(&RetentionAuthority) + Send + Sync>;

/// Boot supplies the existing owners. Installing the boundary is an infallible index-lock update;
/// it must revoke exact reads immediately, including when authority directory synchronization fails.
#[derive(Clone)]
pub struct RetentionContext {
    pub paper_state: Arc<PaperStateDb>,
    pub paper_log: Arc<PaperLog>,
    pub live_journal_path: Option<PathBuf>,
    pub control: mpsc::Sender<OrchestratorControl>,
    pub database_inputs: DatabaseInputs,
    pub install_boundary: InstallBoundary,
    #[cfg(feature = "scenario")]
    pub hooks: Arc<RetentionHooks>,
}

#[cfg(feature = "scenario")]
type AuthorityWriter = Arc<
    dyn Fn(&RetentionAuthority, &Path) -> Result<(), pe_event_log::RetentionWriteError>
        + Send
        + Sync,
>;
#[cfg(feature = "scenario")]
type DirectorySync = Arc<dyn Fn(&Path) -> Result<(), pe_event_log::RetentionError> + Send + Sync>;

#[cfg(feature = "scenario")]
#[derive(Default)]
pub struct RetentionHooks {
    pub after_feed_copy: Option<Arc<dyn Fn() -> std::io::Result<()> + Send + Sync>>,
    pub after_commit: Option<Arc<dyn Fn() -> std::io::Result<()> + Send + Sync>>,
    pub after_publication: Option<Arc<dyn Fn() -> std::io::Result<()> + Send + Sync>>,
    pub mid_punch: Option<Arc<dyn Fn() -> std::io::Result<()> + Send + Sync>>,
    pub authority_write: Option<AuthorityWriter>,
    pub directory_sync: Option<DirectorySync>,
    pub before_index_switch: Option<Arc<dyn Fn() -> std::io::Result<()> + Send + Sync>>,
}

pub struct RetentionCommitRequest {
    context: RetentionContext,
    receipts: SourceReceiptIndex,
    authority: RetentionAuthority,
    expected_epoch: u64,
    cutoff: i64,
    payload_receipts: HashMap<String, Vec<AppendReceipt>>,
}

pub enum RetentionCommitOutcome {
    Deferred(&'static str),
    Committed {
        authority: RetentionAuthority,
        directory_synced: bool,
    },
}

pub(crate) fn pause_reason(context: &RetentionContext) -> anyhow::Result<Option<&'static str>> {
    let era = paper_era(context.paper_log.snapshot()?);
    if era.start.is_none() {
        return Ok(Some("no_start"));
    }
    if !era.frames.iter().any(|frame| {
        matches!(
            &frame.frame,
            PaperLogFrame::Record(PaperLogRecord::QualificationSealed(_))
        )
    }) {
        return Ok(Some("unsealed_start"));
    }
    if (context.database_inputs)()?.dispatch_seeds_exist {
        return Ok(Some("dispatch_seed"));
    }
    if let Some(path) = &context.live_journal_path {
        let events = pe_execution_core::live_journal::replay_all(path)?;
        if crate::decision_replay::live_source_receipts(&events)
            .next()
            .is_some()
        {
            return Ok(Some("live_source_evidence"));
        }
    }
    Ok(None)
}

pub(crate) fn sync_authority(context: &RetentionContext, path: &Path) -> anyhow::Result<()> {
    #[cfg(feature = "scenario")]
    if let Some(sync) = &context.hooks.directory_sync {
        return sync(path).map_err(Into::into);
    }
    let _ = context;
    RetentionAuthority::sync_directory(path)?;
    Ok(())
}

fn cancelled(cancel: &AtomicBool) -> anyhow::Result<()> {
    ensure!(
        !cancel.load(Ordering::Acquire),
        "source retention cancelled"
    );
    Ok(())
}

fn receipt_at(index: &SourceReceiptIndex, sequence: EventSeq) -> anyhow::Result<AppendReceipt> {
    index
        .receipt_at(sequence)?
        .map(|(receipt, _)| receipt)
        .context("retention receipt missing")
}

fn pin_for_receipt(
    index: &SourceReceiptIndex,
    boundary: RetentionBoundary,
    receipt: AppendReceipt,
    reducer: bool,
) -> anyhow::Result<Option<RetentionPin>> {
    if receipt.sequence >= boundary.sequence {
        return Ok(None);
    }
    let tail = index.current_tail_binding()?;
    let authority = RetentionAuthority::load(&tail.path)?;
    if let Some(authority) = &authority {
        ensure!(
            receipt.sequence >= authority.boundary.sequence
                || authority.pin(receipt.sequence).is_some(),
            "retention input was already erased"
        );
    }
    let start = usize::try_from(receipt.sequence.0)?;
    let frame = index
        .checkpoint_suffix(
            start,
            start.checked_add(1).context("receipt overflow")?,
            &LogTailBinding {
                path: tail.path.clone(),
                physical_tail: tail.physical_tail,
                last_sequence: Some(receipt.sequence),
                last_hash: receipt.this_hash,
            },
        )?
        .into_iter()
        .next()
        .context("retention receipt missing")?;
    let offset = frame
        .byte_offset
        .context("retention receipt offset missing")?;
    let predecessor_hash = receipt
        .sequence
        .0
        .checked_sub(1)
        .map(|sequence| receipt_at(index, EventSeq(sequence)).map(|receipt| receipt.this_hash))
        .transpose()?
        .unwrap_or_else(|| blake3::Hash::from_bytes([0; 32]));
    let (envelope, end) = Reader::read_at(&tail.path, offset, receipt.sequence, predecessor_hash)?;
    ensure!(
        envelope.this_hash == receipt.this_hash && end <= boundary.offset,
        "retention pin binding differs"
    );
    let wallet = if reducer && envelope.source_id.0 == ACTIVITY_WS_SOURCE_ID {
        Some(
            pe_source_polymarket_public::parse_activity_trade_observation(&envelope.payload)?
                .wallet,
        )
    } else {
        None
    };
    Ok(Some(RetentionPin {
        sequence: receipt.sequence,
        offset,
        hash: receipt.this_hash,
        predecessor_hash,
        reducer,
        wallet,
    }))
}

fn add_pin(
    pins: &mut BTreeMap<EventSeq, RetentionPin>,
    index: &SourceReceiptIndex,
    boundary: RetentionBoundary,
    receipt: AppendReceipt,
    reducer: bool,
) -> anyhow::Result<()> {
    if let Some(pin) = pin_for_receipt(index, boundary, receipt, reducer)? {
        if let Some(previous) = pins.get_mut(&pin.sequence) {
            ensure!(
                previous.hash == pin.hash
                    && previous.offset == pin.offset
                    && previous.predecessor_hash == pin.predecessor_hash,
                "retention pins conflict"
            );
            previous.reducer |= pin.reducer;
            if pin.wallet.is_some() {
                previous.wallet = pin.wallet;
            }
        } else {
            pins.insert(pin.sequence, pin);
        }
    }
    Ok(())
}

fn collect_receipts(
    value: &serde_json::Value,
    receipts: &mut Vec<AppendReceipt>,
) -> anyhow::Result<()> {
    match value {
        serde_json::Value::Object(object)
            if object.contains_key("sequence") && object.contains_key("this_hash") =>
        {
            receipts.push(serde_json::from_value(value.clone())?);
        }
        serde_json::Value::Object(object) => {
            for value in object.values() {
                collect_receipts(value, receipts)?;
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                collect_receipts(value, receipts)?;
            }
        }
        _ => {}
    }
    Ok(())
}

struct DecisionPins {
    receipts: Vec<AppendReceipt>,
    payload_hashes: BTreeSet<String>,
}

fn decision_pins(
    context: &RetentionContext,
    index: &SourceReceiptIndex,
    cutoff: i64,
) -> anyhow::Result<DecisionPins> {
    let era = paper_era(context.paper_log.snapshot()?);
    let local_last = context.paper_state.financial_last_prepared_seq()?;
    let finals = era
        .frames
        .iter()
        .filter_map(|frame| match &frame.frame {
            PaperLogFrame::Record(PaperLogRecord::FinancialFinal {
                prepared_receipt, ..
            }) => Some(prepared_receipt.sequence),
            _ => None,
        })
        .collect::<HashSet<_>>();
    let rows = context.paper_state.decision_pending_history()?;
    let open = rows
        .iter()
        .filter(|row| row.state == DecisionPendingState::Open)
        .map(|row| row.source_trade_id.clone())
        .collect::<HashSet<_>>();
    let recent = rows
        .iter()
        .filter(|row| row.updated_at_unix >= cutoff)
        .map(|row| row.source_trade_id.clone())
        .collect::<HashSet<_>>();
    let mut financial_decisions = HashSet::new();
    let mut receipts = Vec::new();
    for frame in &era.frames {
        match &frame.frame {
            PaperLogFrame::Record(PaperLogRecord::MembershipChanged { evidence, .. }) => {
                let typed: crate::paper_recovery::SealedMembershipEvidence =
                    serde_json::from_value(evidence.clone())?;
                collect_receipts(&serde_json::to_value(typed)?, &mut receipts)?
            }
            PaperLogFrame::Record(PaperLogRecord::FinancialPrepared { payload, .. }) => {
                let needs_terminal = matches!(payload, FinancialPayload::Fill { operation, .. } if open.contains(&operation.source_trade_id));
                let needs_projection = local_last.is_none_or(|last| frame.receipt.sequence > last)
                    || !finals.contains(&frame.receipt.sequence)
                    || needs_terminal;
                let recent_decision = matches!(payload, FinancialPayload::Fill { operation, .. } if recent.contains(&operation.source_trade_id));
                if needs_projection || recent_decision {
                    match payload {
                        FinancialPayload::Fill {
                            operation,
                            economic,
                        } => {
                            financial_decisions.insert(operation.source_trade_id.clone());
                            receipts.extend(economic_source_receipts(economic));
                        }
                        FinancialPayload::Resolution {
                            resolution_source_receipt,
                            ..
                        } => receipts.push(*resolution_source_receipt),
                    }
                }
            }
            _ => {}
        }
    }
    let mut payload_hashes = BTreeSet::new();
    for row in rows {
        let continuation = crate::bucket_commit::DecisionContinuationV3::from_durable(&row)?;
        if continuation.is_activity_frame() {
            receipts.extend(continuation.observed_source_receipt);
        }
        if row.state == DecisionPendingState::Open
            || row.updated_at_unix >= cutoff
            || financial_decisions.contains(&row.source_trade_id)
        {
            let inputs = decision_source_inputs(&row, index)?;
            receipts.extend(inputs.receipts);
            payload_hashes.extend(inputs.payload_hashes);
        }
    }
    for sequence in (context.database_inputs)()?.identity_sequences {
        receipts.push(receipt_at(index, sequence)?);
    }
    Ok(DecisionPins {
        receipts,
        payload_hashes,
    })
}

#[derive(Deserialize)]
struct ActivityPinProjection {
    by_wallet: HashMap<WalletAddress, BTreeMap<i64, BTreeMap<String, ObligationPin>>>,
    binding_commitments: Vec<AppendReceipt>,
    routed_frames: HashSet<EventSeq>,
    frame_candidates: HashMap<String, AppendReceipt>,
}
#[derive(Deserialize)]
struct ObligationPin {
    receipt: AppendReceipt,
}
#[derive(Deserialize)]
struct BoundaryPinProjection {
    boundaries: Vec<PendingBoundary>,
}

pub(crate) fn observation_wallets(
    frozen: &FrozenCheckpoint,
) -> anyhow::Result<HashSet<WalletAddress>> {
    let activity: ActivityPinProjection =
        serde_json::from_value(serde_json::to_value(&frozen.reducers.activity)?)?;
    Ok(activity.by_wallet.into_keys().collect())
}

fn reducer_pins(
    frozen: &FrozenCheckpoint,
    context: &RetentionContext,
    index: &SourceReceiptIndex,
    boundary: RetentionBoundary,
    pins: &mut BTreeMap<EventSeq, RetentionPin>,
) -> anyhow::Result<HashSet<EventSeq>> {
    let activity: ActivityPinProjection =
        serde_json::from_value(serde_json::to_value(&frozen.reducers.activity)?)?;
    for receipt in activity
        .by_wallet
        .values()
        .flat_map(BTreeMap::values)
        .flat_map(BTreeMap::values)
        .map(|obligation| obligation.receipt)
        .chain(activity.frame_candidates.values().copied())
    {
        add_pin(pins, index, boundary, receipt, true)?;
    }
    for receipt in activity.binding_commitments {
        add_pin(pins, index, boundary, receipt, true)?;
        for dependency in commitment_source_receipts(receipt, index)? {
            add_pin(pins, index, boundary, dependency, false)?;
        }
    }
    if let Some(candidates) = &frozen.reducers.daily_boundary {
        let projection: BoundaryPinProjection =
            serde_json::from_value(serde_json::to_value(candidates)?)?;
        let era = paper_era(context.paper_log.snapshot()?);
        let mut obligations = crate::trade_poller::ReconciliationObligations::default();
        if let Some(anchor) =
            crate::trade_poller::recover_daily_boundary_anchor_from_era(&era, &mut obligations)?
        {
            for boundary_candidate in projection
                .boundaries
                .into_iter()
                .filter(|candidate| candidate.cutoff_unix > anchor)
            {
                add_pin(pins, index, boundary, boundary_candidate.receipt, true)?;
            }
        }
    }
    Ok(activity.routed_frames)
}

fn routed_artifact(envelope: &EventEnvelope, routed: &HashSet<EventSeq>) -> anyhow::Result<bool> {
    if envelope.source_id.0 != crate::frame_admission::FRAME_FALLBACK_SOURCE_ID {
        return Ok(false);
    }
    let artifact: crate::frame_admission::FrameFallbackArtifact =
        serde_json::from_slice(&envelope.payload)?;
    Ok(routed.contains(&artifact.frame_receipt.sequence))
}

pub(crate) enum PreparedRetention {
    Deferred(&'static str),
    Nothing {
        tail: LogTailBinding,
        boundary_current: bool,
    },
    Advance(Box<RetentionCommitRequest>),
}

pub(crate) fn prepare(
    frozen: &FrozenCheckpoint,
    context: &RetentionContext,
    index: &SourceReceiptIndex,
    now: i64,
    cancel: &AtomicBool,
) -> anyhow::Result<PreparedRetention> {
    if let Some(reason) = pause_reason(context)? {
        return Ok(PreparedRetention::Deferred(reason));
    }
    cancelled(cancel)?;
    let path = &frozen.tail.path;
    let authority = RetentionAuthority::load(path)?;
    let epoch = authority.as_ref().map_or(0, |authority| authority.epoch);
    let old_boundary = authority.as_ref().map_or(
        RetentionBoundary {
            sequence: EventSeq(0),
            offset: pe_event_log::HEADER_LEN,
        },
        |authority| authority.boundary,
    );
    if authority.as_ref().is_some_and(|authority| {
        now.saturating_sub(authority.advanced_at) < SOURCE_RETENTION_ADVANCE_SECS
    }) {
        return Ok(PreparedRetention::Deferred("advance_not_due"));
    }
    let tail = index.current_tail_binding()?;
    let count = super::tail_receipt_count(&tail)?;
    let frames = index.checkpoint_suffix(0, count, &tail)?;
    let cutoff = now
        .checked_sub(SOURCE_RETENTION_BUFFER_SECS)
        .context("retention clock overflow")?;
    let cutoff_millis = cutoff
        .checked_mul(1000)
        .context("retention clock overflow")?;
    let first = frames
        .iter()
        .find(|frame| frame.received_millis >= cutoff_millis)
        .or_else(|| frames.last());
    let Some(first) = first else {
        return Ok(PreparedRetention::Nothing {
            tail,
            boundary_current: true,
        });
    };
    let boundary = RetentionBoundary {
        sequence: first.receipt.sequence,
        offset: first.byte_offset.context("boundary offset missing")?,
    };
    if boundary.sequence <= old_boundary.sequence {
        return Ok(PreparedRetention::Nothing {
            tail,
            boundary_current: true,
        });
    }
    // Reducer pins and finishing both start from the frozen snapshot, so it must already reach
    // past the boundary. A frame appended after the last capture waits for the next capture.
    if frozen.tail.physical_tail <= boundary.offset {
        return Ok(PreparedRetention::Deferred("snapshot_behind_boundary"));
    }
    let mut pins = BTreeMap::new();
    let decisions = decision_pins(context, index, cutoff)?;
    for receipt in decisions.receipts {
        cancelled(cancel)?;
        add_pin(&mut pins, index, boundary, receipt, false)?;
    }
    let activation_sequence = frozen
        .activation
        .last_sequence
        .context("retention requires a recorded migration activation frame")?;
    add_pin(
        &mut pins,
        index,
        boundary,
        receipt_at(index, activation_sequence)?,
        false,
    )?;
    let routed = reducer_pins(frozen, context, index, boundary, &mut pins)?;
    let mut payload_receipts: HashMap<String, Vec<AppendReceipt>> = HashMap::new();
    if let Some(authority) = &authority {
        for pin in &authority.pins {
            cancelled(cancel)?;
            let (envelope, _) = authority.verify_pin(path, pin)?;
            if routed_artifact(&envelope, &routed)? {
                add_pin(
                    &mut pins,
                    index,
                    boundary,
                    AppendReceipt {
                        sequence: envelope.seq,
                        this_hash: envelope.this_hash,
                    },
                    true,
                )?;
            }
            if envelope.source_id.0 == crate::clob_book::CLOB_BOOK_SOURCE_ID {
                payload_receipts
                    .entry(envelope.raw_payload_hash.to_hex().to_string())
                    .or_default()
                    .push(AppendReceipt {
                        sequence: envelope.seq,
                        this_hash: envelope.this_hash,
                    });
            }
        }
    }
    // The archive and authority share the checkpoint lock. Preparation may publish while the
    // log copy runs, but another retention epoch cannot make this epoch's archive committed.
    let _lock = CheckpointLock::acquire(path)?;
    ensure!(
        super::retention_epoch(path)? == epoch,
        "retention epoch changed during preparation"
    );
    let next_epoch = epoch.checked_add(1).context("retention epoch exhausted")?;
    let mut feed = FeedArchiveWriter::new(path, next_epoch)?;
    let end = LogTailBinding {
        path: tail.path.clone(),
        physical_tail: boundary.offset,
        last_sequence: boundary.sequence.0.checked_sub(1).map(EventSeq),
        last_hash: receipt_at(
            index,
            EventSeq(
                boundary
                    .sequence
                    .0
                    .checked_sub(1)
                    .context("boundary predecessor missing")?,
            ),
        )?
        .this_hash,
    };
    let mut walk_error = None;
    let mut digest = blake3::Hasher::new();
    let actual = Scanner::walk_bounded_cancellable(
        path,
        boundary.offset,
        &end,
        None,
        &mut digest,
        &mut |offset, envelope| {
            if walk_error.is_some() {
                return;
            }
            let receipt = AppendReceipt {
                sequence: envelope.seq,
                this_hash: envelope.this_hash,
            };
            let observed = (|| -> anyhow::Result<()> {
                if envelope.source_id.0 == ACTIVITY_WS_SOURCE_ID {
                    feed.append(offset, envelope.seq, envelope.prev_hash)?;
                }
                if routed_artifact(envelope, &routed)? {
                    add_pin(&mut pins, index, boundary, receipt, true)?;
                }
                if envelope.source_id.0 == crate::clob_book::CLOB_BOOK_SOURCE_ID {
                    payload_receipts
                        .entry(envelope.raw_payload_hash.to_hex().to_string())
                        .or_default()
                        .push(receipt);
                }
                Ok(())
            })();
            if let Err(error) = observed {
                walk_error = Some(error);
            }
        },
        Some(cancel),
    )?;
    ensure!(actual == end, "retention feed walk binding differs");
    if let Some(error) = walk_error {
        return Err(error);
    }
    let entry = feed.finish()?;
    #[cfg(feature = "scenario")]
    if let Some(hook) = &context.hooks.after_feed_copy {
        hook()?;
    }
    add_payload_pins(
        &decisions.payload_hashes,
        &payload_receipts,
        &mut pins,
        index,
        boundary,
    )?;
    // Receipts must be authentic and durable before an authority can make them retention metadata.
    ensure_receipts(path, &frames, boundary)?;
    if context.paper_state.sync_checkpoint_dispositions().is_err() {
        return Ok(PreparedRetention::Deferred("disposition_barrier"));
    }
    ensure!(
        matches!(super::read_authority(path)?, Authority::Readable(record) if record.retention_fence),
        "retention fence missing"
    );
    let captured_tail = index.current_tail_binding()?;
    File::open(path)?.sync_all()?;
    let mut feed_entries = authority
        .as_ref()
        .map_or_else(Vec::new, |authority| authority.feed.clone());
    if let Some(entry) = entry {
        feed_entries.push(entry);
    }
    Ok(PreparedRetention::Advance(Box::new(
        RetentionCommitRequest {
            context: context.clone(),
            receipts: index.clone(),
            expected_epoch: epoch,
            cutoff,
            payload_receipts,
            authority: RetentionAuthority {
                format_version: 1,
                epoch: next_epoch,
                advanced_at: now,
                boundary,
                chain_head: end.last_hash,
                pins: pins.into_values().collect(),
                retained_tail: TailBinding::from(&captured_tail),
                feed: feed_entries,
            },
        },
    )))
}

fn add_payload_pins(
    hashes: &BTreeSet<String>,
    payload_receipts: &HashMap<String, Vec<AppendReceipt>>,
    pins: &mut BTreeMap<EventSeq, RetentionPin>,
    index: &SourceReceiptIndex,
    boundary: RetentionBoundary,
) -> anyhow::Result<()> {
    for hash in hashes {
        if let Some(receipts) = payload_receipts.get(hash) {
            for receipt in receipts {
                add_pin(pins, index, boundary, *receipt, false)?;
            }
        }
    }
    Ok(())
}

fn ensure_receipts(
    path: &Path,
    frames: &[SourceFrameMetadata],
    boundary: RetentionBoundary,
) -> anyhow::Result<()> {
    let count = usize::try_from(boundary.sequence.0)?;
    let file = File::open(super::receipts_path(path))?;
    let mut reader = std::io::BufReader::new(&file);
    for (sequence, frame) in frames.iter().take(count).enumerate() {
        let record = super::read_record(&mut reader, sequence)?;
        ensure!(
            record.receipt == frame.receipt
                && record.received_millis == frame.received_millis
                && record.byte_offset == frame.byte_offset,
            "retention receipts differ from index"
        );
    }
    ensure!(frames.len() >= count, "retention receipts incomplete");
    file.sync_all()?;
    Ok(())
}

/// Applied inside the orchestrator's dispatch owner, never concurrently with seed creation.
pub(crate) fn commit(
    mut request: RetentionCommitRequest,
) -> anyhow::Result<RetentionCommitOutcome> {
    let path = request.receipts.current_tail_binding()?.path;
    let _lock = CheckpointLock::acquire(&path)?;
    if super::retention_epoch(&path)? != request.expected_epoch {
        return Ok(RetentionCommitOutcome::Deferred("epoch_changed"));
    }
    if let Some(reason) = pause_reason(&request.context)? {
        return Ok(RetentionCommitOutcome::Deferred(reason));
    }
    ensure!(
        matches!(super::read_authority(&path)?, Authority::Readable(record) if record.retention_fence),
        "retention fence missing"
    );
    let mut pins = request
        .authority
        .pins
        .iter()
        .cloned()
        .map(|pin| (pin.sequence, pin))
        .collect::<BTreeMap<_, _>>();
    let decisions = decision_pins(&request.context, &request.receipts, request.cutoff)?;
    for receipt in decisions.receipts {
        add_pin(
            &mut pins,
            &request.receipts,
            request.authority.boundary,
            receipt,
            false,
        )?;
    }
    add_payload_pins(
        &decisions.payload_hashes,
        &request.payload_receipts,
        &mut pins,
        &request.receipts,
        request.authority.boundary,
    )?;
    request.authority.pins = pins.into_values().collect();
    let result = {
        #[cfg(feature = "scenario")]
        if let Some(write) = &request.context.hooks.authority_write {
            write(&request.authority, &path)
        } else {
            request.authority.write(&path)
        }
        #[cfg(not(feature = "scenario"))]
        request.authority.write(&path)
    };
    let directory_synced = match result {
        Ok(()) => true,
        Err(error) if error.visible => false,
        Err(error) => return Err(error.into()),
    };
    // Even a fault at the switch seam cannot leave a visibly committed epoch using the old index.
    #[cfg(feature = "scenario")]
    let switch_result = request
        .context
        .hooks
        .before_index_switch
        .as_ref()
        .map(|hook| hook());
    (request.context.install_boundary)(&request.authority);
    info!(
        epoch = request.authority.epoch,
        boundary_sequence = request.authority.boundary.sequence.0,
        boundary_offset = request.authority.boundary.offset,
        "source retention committed"
    );
    #[cfg(feature = "scenario")]
    let directory_synced = directory_synced && switch_result.is_none_or(|result| result.is_ok());
    Ok(RetentionCommitOutcome::Committed {
        authority: request.authority,
        directory_synced,
    })
}

pub(crate) fn send_commit(
    request: RetentionCommitRequest,
) -> anyhow::Result<RetentionCommitOutcome> {
    let (acknowledged, response) = oneshot::channel();
    request
        .context
        .control
        .clone()
        .blocking_send(OrchestratorControl::RetentionCommit {
            request: Box::new(request),
            acknowledged,
        })
        .map_err(|_| anyhow::anyhow!("retention orchestrator stopped"))?;
    response
        .blocking_recv()
        .context("retention commit acknowledgement dropped")?
        .map_err(anyhow::Error::msg)
}

/// Verify the complete bounded retained window and collect only this job's wallet witnesses.
pub(crate) fn verified_window(
    frozen: &mut FrozenCheckpoint,
    authority: Option<&RetentionAuthority>,
    tail: LogTailBinding,
    cancel: &AtomicBool,
) -> anyhow::Result<HashSet<WalletAddress>> {
    if let Some(authority) = authority {
        for pin in &authority.pins {
            cancelled(cancel)?;
            authority.verify_pin(&tail.path, pin)?;
        }
    }
    let mut digest = blake3::Hasher::new();
    let mut wallets = HashSet::new();
    let mut observe_error = None;
    let actual = Scanner::walk_bounded_cancellable(
        &tail.path,
        tail.physical_tail,
        &tail,
        None,
        &mut digest,
        &mut |_, envelope| {
            if observe_error.is_some() {
                return;
            }
            let wallet = (|| -> anyhow::Result<Option<WalletAddress>> {
                if envelope.source_id.0 == ACTIVITY_WS_SOURCE_ID {
                    Ok(Some(
                        pe_source_polymarket_public::parse_activity_trade_observation(
                            &envelope.payload,
                        )?
                        .wallet,
                    ))
                } else if envelope.source_id.0
                    == crate::bucket_commit::ACTIVITY_READ_COMMITMENT_SOURCE_ID
                {
                    let commitment: crate::bucket_commit::ActivityReadCommitment =
                        serde_json::from_slice(&envelope.payload)?;
                    Ok(Some(commitment.wallet))
                } else {
                    Ok(None)
                }
            })();
            match wallet {
                Ok(Some(wallet)) => {
                    wallets.insert(wallet);
                }
                Ok(None) => {}
                Err(error) => observe_error = Some(error),
            }
        },
        Some(cancel),
    )?;
    ensure!(
        actual == tail,
        "retention window binding differs from captured tail"
    );
    if let Some(error) = observe_error {
        return Err(error);
    }
    frozen.tail = tail;
    frozen.prefix = FrozenPrefix::Verified(Box::new(digest));
    Ok(wallets)
}

pub(crate) fn punch(
    path: &Path,
    authority: &RetentionAuthority,
    cancel: &AtomicBool,
    #[cfg(feature = "scenario")] hooks: &RetentionHooks,
) -> anyhow::Result<u64> {
    let file = std::fs::OpenOptions::new().write(true).open(path)?;
    let mut cursor = PUNCH_BLOCK;
    let mut bytes = 0u64;
    for pin in &authority.pins {
        cancelled(cancel)?;
        let (_, end) = authority.verify_pin(path, pin)?;
        let punched = punch_range(&file, cursor, pin.offset)?;
        bytes = bytes
            .checked_add(punched)
            .context("punch byte count overflow")?;
        cursor = cursor.max(end);
        #[cfg(feature = "scenario")]
        if punched > 0
            && let Some(hook) = &hooks.mid_punch
        {
            hook()?;
        }
    }
    cancelled(cancel)?;
    let punched = punch_range(&file, cursor, authority.boundary.offset)?;
    bytes = bytes
        .checked_add(punched)
        .context("punch byte count overflow")?;
    #[cfg(feature = "scenario")]
    if punched > 0
        && let Some(hook) = &hooks.mid_punch
    {
        hook()?;
    }
    file.sync_all()?;
    Ok(bytes)
}

fn punch_range(file: &File, start: u64, end: u64) -> anyhow::Result<u64> {
    let start = start
        .checked_add(PUNCH_BLOCK - 1)
        .context("punch offset overflow")?
        / PUNCH_BLOCK
        * PUNCH_BLOCK;
    let end = end / PUNCH_BLOCK * PUNCH_BLOCK;
    if end <= start {
        return Ok(0);
    }
    rustix::fs::fallocate(
        file,
        rustix::fs::FallocateFlags::PUNCH_HOLE | rustix::fs::FallocateFlags::KEEP_SIZE,
        start,
        end - start,
    )?;
    Ok(end - start)
}

/// Drive the same single-owner commit handler in deterministic control-channel fixtures.
#[cfg(feature = "scenario")]
pub fn commit_for_scenario(
    request: RetentionCommitRequest,
) -> anyhow::Result<RetentionCommitOutcome> {
    commit(request)
}
