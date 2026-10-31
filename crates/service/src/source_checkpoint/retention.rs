//! Daily source retention, serialized with live dispatch by the orchestrator.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Context as _, ensure};
use pe_core_types::{EventSeq, SourceTradeId, WalletAddress};
use pe_event_log::{
    AppendReceipt, EventEnvelope, FeedArchiveWriter, LogTailBinding, Reader, RetentionAuthority,
    RetentionBoundary, RetentionPin, Scanner, TailBinding,
};
use pe_paper_state::{DecisionPendingRow, DecisionPendingState, PaperStateDb};
use serde::Deserialize;
use tokio::sync::{mpsc, oneshot};
use tracing::info;

use super::{Authority, CheckpointLock};
use crate::activity_ingest::ACTIVITY_WS_SOURCE_ID;
use crate::decision_replay::{
    DecisionSourceInputs, commitment_source_receipts, decision_source_inputs,
    economic_source_receipts,
};
use crate::orchestrator_control::OrchestratorControl;
use crate::paper_recovery::{
    FinancialPayload, PaperLog, PaperLogFrame, PaperLogRecord, generation_qualification_sealed,
    paper_era, paper_generation,
};
use crate::risk_inputs::{
    BoundaryCandidate, ReceiptCommitment, RetentionAudit, SourceReceiptIndex,
};
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

impl RetentionDatabaseInputs {
    pub fn read(paper_state: &PaperStateDb) -> anyhow::Result<Self> {
        let identity_sequences = paper_state
            .asset_identity_source_sequences()?
            .into_iter()
            .map(|sequence| u64::try_from(sequence).map(EventSeq))
            .collect::<Result<Vec<_>, _>>()
            .context("asset identity source sequence is negative")?;
        Ok(Self {
            identity_sequences,
            dispatch_seeds_exist: paper_state.dispatch_seeds_exist()?,
        })
    }
}

type DatabaseInputs = Arc<dyn Fn() -> anyhow::Result<RetentionDatabaseInputs> + Send + Sync>;

/// Boot supplies the existing paper and orchestrator owners.
#[derive(Clone)]
pub struct RetentionContext {
    pub paper_state: Arc<PaperStateDb>,
    pub paper_log: Arc<PaperLog>,
    pub live_journal_path: Option<PathBuf>,
    pub control: mpsc::Sender<OrchestratorControl>,
    pub database_inputs: DatabaseInputs,
    #[cfg(feature = "scenario")]
    pub hooks: Arc<RetentionHooks>,
}

#[cfg(feature = "scenario")]
type AuthorityWriter = Arc<
    dyn Fn(pe_event_log::StagedRetention) -> Result<(), pe_event_log::RetentionWriteError>
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
    expected_generation: Option<u64>,
    cutoff: i64,
    payload_receipts: HashMap<String, Vec<AppendReceipt>>,
    decision_rows: DecisionPinCache,
}

pub enum RetentionCommitOutcome {
    Deferred(&'static str),
    Committed {
        authority: RetentionAuthority,
        directory_synced: bool,
    },
}

pub(crate) fn pause_reason(context: &RetentionContext) -> anyhow::Result<Option<&'static str>> {
    let frames = context.paper_log.snapshot()?;
    if !frames.iter().any(|frame| {
        matches!(
            frame.frame,
            PaperLogFrame::Record(PaperLogRecord::QualificationStarted(_))
        )
    }) {
        return Ok(Some("no_start"));
    }
    if !generation_qualification_sealed(&frames) {
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

/// `current` is the authority in force, loaded once by the calling preparation or commit.
fn pin_for_receipt(
    index: &SourceReceiptIndex,
    current: Option<&RetentionAuthority>,
    boundary: RetentionBoundary,
    receipt: AppendReceipt,
    reducer: bool,
) -> anyhow::Result<Option<RetentionPin>> {
    if receipt.sequence >= boundary.sequence {
        return Ok(None);
    }
    let tail = index.current_tail_binding()?;
    if let Some(authority) = current {
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

/// Every entry of `pins` was authenticated against `boundary` by the pin build that owns the map or by the
/// preparation that produced the commit request, so a receipt already pinned with the same hash is not read
/// again unless it needs the reducer role.
fn add_pin(
    pins: &mut BTreeMap<EventSeq, RetentionPin>,
    index: &SourceReceiptIndex,
    current: Option<&RetentionAuthority>,
    boundary: RetentionBoundary,
    receipt: AppendReceipt,
    reducer: bool,
) -> anyhow::Result<()> {
    if pins
        .get(&receipt.sequence)
        .is_some_and(|pin| pin.hash == receipt.this_hash && (pin.reducer || !reducer))
    {
        return Ok(());
    }
    if let Some(pin) = pin_for_receipt(index, current, boundary, receipt, reducer)? {
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
    rows: usize,
    derived: usize,
}

/// One decision row's share of the decision pins, kept with the exact row it was derived from.
struct DecisionRowPins {
    row: DecisionPendingRow,
    observed: Option<AppendReceipt>,
    inputs: Option<DecisionSourceInputs>,
}

/// Decision pins by row. Preparation derives every row off the serving path; the commit runs inside the
/// orchestrator, so it reuses an entry while its row is unchanged and derives only rows written since.
#[derive(Default)]
struct DecisionPinCache(HashMap<SourceTradeId, DecisionRowPins>);

fn decision_pins(
    context: &RetentionContext,
    index: &SourceReceiptIndex,
    cutoff: i64,
    cache: &mut DecisionPinCache,
) -> anyhow::Result<DecisionPins> {
    let frames = context.paper_log.snapshot()?;
    let generation = paper_generation(frames.clone());
    let era = paper_era(frames);
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
    for frame in &generation.frames {
        if let PaperLogFrame::Record(PaperLogRecord::MembershipChanged { evidence, .. }) =
            &frame.frame
        {
            let typed: crate::paper_recovery::SealedMembershipEvidence =
                serde_json::from_value(evidence.clone())?;
            collect_receipts(&serde_json::to_value(typed)?, &mut receipts)?;
        }
    }
    for frame in &era.frames {
        if let PaperLogFrame::Record(PaperLogRecord::FinancialPrepared { payload, .. }) =
            &frame.frame
        {
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
    }
    let mut payload_hashes = BTreeSet::new();
    let mut previous = std::mem::take(&mut cache.0);
    let total = rows.len();
    let mut derived = 0;
    for row in rows {
        let mut entry = match previous.remove(&row.source_trade_id) {
            Some(entry) if entry.row == row => entry,
            _ => {
                let continuation =
                    crate::bucket_commit::DecisionContinuationV3::from_durable(&row)?;
                DecisionRowPins {
                    observed: continuation
                        .observed_source_receipt
                        .filter(|_| continuation.is_activity_frame()),
                    row,
                    inputs: None,
                }
            }
        };
        receipts.extend(entry.observed);
        if entry.row.state == DecisionPendingState::Open
            || entry.row.updated_at_unix >= cutoff
            || financial_decisions.contains(&entry.row.source_trade_id)
        {
            if entry.inputs.is_none() {
                entry.inputs = Some(decision_source_inputs(&entry.row, index)?);
                derived += 1;
            }
            if let Some(inputs) = &entry.inputs {
                receipts.extend(inputs.receipts.iter().copied());
                payload_hashes.extend(inputs.payload_hashes.iter().cloned());
            }
        }
        cache.0.insert(entry.row.source_trade_id.clone(), entry);
    }
    for sequence in (context.database_inputs)()?.identity_sequences {
        receipts.push(receipt_at(index, sequence)?);
    }
    Ok(DecisionPins {
        receipts,
        payload_hashes,
        rows: total,
        derived,
    })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use pe_core_types::{BasisPoints, CollateralAmount, ReceivedAt, SourceId, SourceTimestamp};
    use pe_event_log::{ContentType, EnvelopeIn, Writer};
    use pe_trader_index::{Watchlist, WatchlistEntry, WatchlistTier};
    use rust_decimal_macros::dec;
    use time::OffsetDateTime;

    use super::*;
    use crate::paper_recovery::{
        CanonicalResolutionResult, CapacityMembershipArtifact, ExpectedAuthority, FinancialResult,
        MembershipReason, QualificationSealed, QualificationStarted, SealReason,
        SealedMembershipEvidence, TailBinding as PaperTailBinding, replay_membership_with_source,
    };

    const NOW: i64 = 1_800_000_000;
    const OLD: i64 = NOW - 8 * 86_400;

    fn input(source_id: &str, payload: Vec<u8>, unix: i64, schema_version: u32) -> EnvelopeIn {
        let at = OffsetDateTime::from_unix_timestamp(unix).unwrap();
        EnvelopeIn {
            source_id: SourceId(source_id.to_owned()),
            schema_version,
            parser_version: 1,
            observed_at: SourceTimestamp(at),
            received_at: ReceivedAt(at),
            content_type: ContentType::Json,
            payload,
        }
    }

    fn append(paper: &PaperLog, record: &PaperLogRecord) -> AppendReceipt {
        paper
            .append_synced(input("paper", serde_json::to_vec(record).unwrap(), OLD, 2))
            .unwrap()
    }

    fn entry(wallet: WalletAddress) -> WatchlistEntry {
        WatchlistEntry {
            wallet,
            tier: WatchlistTier::Active,
            leader_score_bps: BasisPoints(500),
            lcb_5pct_bps: BasisPoints(500),
            win_rate_bps: BasisPoints(6_000),
            closed_trades_in_window: 20,
            reconstruction_quality: pe_core_types::ReconstructionQuality::new(100).unwrap(),
        }
    }

    #[test]
    fn successor_retention_advances_with_generation_membership_and_retired_prior_economics() {
        let dir = tempfile::tempdir().unwrap();
        let source_path = dir.path().join("source.log");
        let mut writer = Writer::open(&source_path).unwrap();
        let activation_receipt = writer
            .append_synced(input("activation", b"activation".to_vec(), OLD, 1))
            .unwrap();
        let activation = writer.verified_tail().unwrap();
        let retired_economic = writer
            .append_synced(input("resolution", b"old economics".to_vec(), OLD, 1))
            .unwrap();
        let first_wallet = WalletAddress([1; 20]);
        let second_wallet = WalletAddress([2; 20]);
        let membership = writer
            .append_synced(input(
                crate::watchlist_admission::CAPACITY_CONFIG_SOURCE_ID,
                serde_json::to_vec(&CapacityMembershipArtifact {
                    generation: 1,
                    target: 1,
                    published_entries: vec![entry(first_wallet)],
                })
                .unwrap(),
                OLD,
                crate::watchlist_admission::MEMBERSHIP_ARTIFACT_SCHEMA_VERSION,
            ))
            .unwrap();
        writer
            .append_synced(input("old", b"old".to_vec(), OLD, 1))
            .unwrap();
        writer
            .append_synced(input("recent", b"recent".to_vec(), NOW, 1))
            .unwrap();
        drop(writer);
        let index = SourceReceiptIndex::replay(&source_path).unwrap();
        let tail = index.current_tail_binding().unwrap();
        let metadata = index.checkpoint_suffix(0, 5, &tail).unwrap();
        std::fs::write(
            crate::source_checkpoint::receipts_path(&source_path),
            metadata
                .iter()
                .flat_map(crate::source_checkpoint::encode_record)
                .collect::<Vec<_>>(),
        )
        .unwrap();
        crate::source_checkpoint::install_retention_fence(&source_path).unwrap();
        let old_boundary = RetentionBoundary {
            sequence: membership.sequence,
            offset: metadata[2].byte_offset.unwrap(),
        };
        let old_authority = RetentionAuthority {
            format_version: 1,
            epoch: 1,
            advanced_at: OLD,
            boundary: old_boundary,
            chain_head: retired_economic.this_hash,
            pins: vec![
                pin_for_receipt(&index, None, old_boundary, activation_receipt, false)
                    .unwrap()
                    .unwrap(),
            ],
            retained_tail: TailBinding::from(&tail),
            feed: Vec::new(),
        };
        old_authority.write(&source_path).unwrap();
        index.install_retention(old_authority).unwrap();
        assert!(matches!(
            index.source_envelope(retired_economic),
            Err(crate::risk_inputs::RiskInputsUnavailable::Erased)
        ));

        let paper = Arc::new(PaperLog::open(dir.path().join("paper.log")).unwrap());
        let empty = PaperTailBinding::from(&paper.verified_tail().unwrap());
        let mut started = QualificationStarted {
            starting_bankroll: CollateralAmount::from_decimal_exact(dec!(100)).unwrap(),
            paper_prefix: empty.clone(),
            source_prefix: PaperTailBinding::from(&activation),
            live_prefix: empty.clone(),
            artifact_blake3: "artifact".to_owned(),
            static_config_hash: "static".to_owned(),
            hot_config_hash: "hot".to_owned(),
            generation: "generation".to_owned(),
            activation_id: "first".to_owned(),
            ranking_batch_id: 7,
            membership: vec![first_wallet, second_wallet],
            membership_proofs_hash: "proof".to_owned(),
            schema_version: 3,
            parser_version: 1,
            financial_semantic_version: 3,
        };
        let first = append(
            &paper,
            &PaperLogRecord::QualificationStarted(Arc::new(started.clone())),
        );
        let prepared = append(
            &paper,
            &PaperLogRecord::FinancialPrepared {
                expected_authority: ExpectedAuthority {
                    qualification_start_receipt: first,
                    prior_completed_prepared_sequence: None,
                },
                payload: FinancialPayload::Resolution {
                    condition_id: pe_core_types::PolymarketConditionId("condition".to_owned()),
                    payout_by_outcome_index_json: "[\"1\",\"0\"]".to_owned(),
                    resolution_source_receipt: retired_economic,
                },
            },
        );
        append(
            &paper,
            &PaperLogRecord::FinancialFinal {
                prepared_receipt: prepared,
                result: FinancialResult::Resolution {
                    canonical: CanonicalResolutionResult {
                        outcome: "applied".to_owned(),
                        bankroll: dec!(100),
                        applied_prepared_seq: prepared.sequence,
                        credit: CollateralAmount::ZERO,
                        settled_at_unix: OLD,
                    },
                },
            },
        );
        let membership_record = append(
            &paper,
            &PaperLogRecord::MembershipChanged {
                reason: MembershipReason::CapacityChange,
                removed: vec![second_wallet],
                added: Vec::new(),
                capacity: 1,
                ranking_batch_id: None,
                evidence: SealedMembershipEvidence::capacity_change(1, membership, Vec::new())
                    .unwrap(),
            },
        );
        append(
            &paper,
            &PaperLogRecord::QualificationSealed(Box::new(QualificationSealed {
                start_receipt: first,
                source_prefix: PaperTailBinding::from(&activation),
                financial_prefix: PaperTailBinding::from(&paper.verified_tail().unwrap()),
                live_prefix: empty,
                decision_evidence_digest: "digest".to_owned(),
                sealed_cutoff_unix: OLD,
                reason: SealReason::Complete,
            })),
        );
        let before = crate::database_retention::retention_paper_state(&paper.snapshot().unwrap());
        assert_eq!(before.1, Some(prepared.sequence));
        started.activation_id = "successor".to_owned();
        started.membership.clear();
        started.paper_prefix = PaperTailBinding::from(&paper.verified_tail().unwrap());
        let successor = append(
            &paper,
            &PaperLogRecord::QualificationStarted(Arc::new(started)),
        );
        let after = crate::database_retention::retention_paper_state(&paper.snapshot().unwrap());
        assert_eq!(after.1, None);
        assert_eq!(before.0, after.0);
        assert_eq!(after.0[&second_wallet].0, membership_record.sequence);
        let state = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
        state.seed_financial_start(successor).unwrap();
        let (control, _rx) = mpsc::channel(1);
        let context = RetentionContext {
            paper_state: state,
            paper_log: paper.clone(),
            live_journal_path: None,
            control,
            database_inputs: Arc::new(|| Ok(RetentionDatabaseInputs::default())),
            #[cfg(feature = "scenario")]
            hooks: Arc::new(RetentionHooks::default()),
        };
        assert_eq!(pause_reason(&context).unwrap(), None);
        let decisions = decision_pins(
            &context,
            &index,
            NOW - SOURCE_RETENTION_BUFFER_SECS,
            &mut DecisionPinCache::default(),
        )
        .unwrap();
        assert!(decisions.receipts.contains(&membership));
        assert!(!decisions.receipts.contains(&retired_economic));
        let frozen = FrozenCheckpoint {
            authority_generation: Some(0),
            capture_unix_ms: u64::try_from(NOW).unwrap() * 1000,
            financial_era: true,
            activation,
            tail: tail.clone(),
            prefix: FrozenPrefix::Verified(Box::new(blake3::Hasher::new())),
            reducers: crate::source_log_boot::Reducers::new(true),
        };
        let PreparedRetention::Advance(request) =
            prepare(&frozen, &context, &index, &[], NOW, &AtomicBool::new(false)).unwrap()
        else {
            unreachable!();
        };
        assert_eq!(request.authority.boundary.sequence, EventSeq(4));
        assert!(request.authority.pin(membership.sequence).is_some());
        assert!(request.authority.pin(retired_economic.sequence).is_none());
        assert!(matches!(
            commit(*request).unwrap(),
            RetentionCommitOutcome::Committed { .. }
        ));
        let authority = RetentionAuthority::load(&source_path).unwrap().unwrap();
        assert_eq!(authority.epoch, 2);
        let boot_index = SourceReceiptIndex::replay(&source_path).unwrap();
        let generation = paper_generation(
            crate::paper_recovery::scan_paper_log(&dir.path().join("paper.log")).unwrap(),
        );
        let replayed = replay_membership_with_source(
            &generation,
            Watchlist {
                entries: vec![entry(first_wallet), entry(second_wallet)],
                snapshot_at: SourceTimestamp(OffsetDateTime::UNIX_EPOCH),
                active_count: 2,
                incubator_count: 0,
            },
            &crate::qualification::PublishedMembershipSource::from_index(boot_index),
        )
        .unwrap()
        .unwrap();
        assert_eq!(replayed.watchlist.entries.len(), 1);
        assert_eq!(replayed.watchlist.entries[0].wallet, first_wallet);
    }

    /// Writes `frames` and one recent frame; returns their receipts, the replayed index and offsets.
    fn pin_fixture(
        path: &Path,
        frames: Vec<EnvelopeIn>,
    ) -> (Vec<AppendReceipt>, SourceReceiptIndex, Vec<u64>) {
        let mut writer = Writer::open(path).unwrap();
        let mut receipts = frames
            .into_iter()
            .map(|frame| writer.append_synced(frame).unwrap())
            .collect::<Vec<_>>();
        receipts.push(
            writer
                .append_synced(input("recent", b"recent".to_vec(), NOW, 1))
                .unwrap(),
        );
        drop(writer);
        let index = SourceReceiptIndex::replay(path).unwrap();
        let tail = index.current_tail_binding().unwrap();
        let offsets = index
            .checkpoint_suffix(0, receipts.len(), &tail)
            .unwrap()
            .iter()
            .map(|frame| frame.byte_offset.unwrap())
            .collect();
        (receipts, index, offsets)
    }

    fn boundary_at(
        receipts: &[AppendReceipt],
        offsets: &[u64],
        position: usize,
    ) -> RetentionBoundary {
        RetentionBoundary {
            sequence: receipts[position].sequence,
            offset: offsets[position],
        }
    }

    #[test]
    fn a_pinned_receipt_is_reused_without_rereading_its_frame() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source.log");
        let wallet = WalletAddress([0xab; 20]);
        let mut activity = input(
            ACTIVITY_WS_SOURCE_ID,
            format!(
                r#"{{"proxyWallet":"0x{}","conditionId":"0x{}","asset":"123","side":"BUY","size":"100","price":"0.50","timestamp":"{OLD}","transactionHash":"0xpinned","outcomeIndex":"0"}}"#,
                "ab".repeat(20),
                "11".repeat(32),
            )
            .into_bytes(),
            OLD,
            pe_source_polymarket_public::ACTIVITY_SCHEMA_VERSION,
        );
        activity.parser_version = pe_source_polymarket_public::ACTIVITY_PARSER_VERSION;
        let (receipts, index, offsets) = pin_fixture(
            &path,
            vec![activity, input("old", b"plain".to_vec(), OLD, 1)],
        );
        let (observed, plain) = (receipts[0], receipts[1]);
        let boundary = boundary_at(&receipts, &offsets, 2);
        let mut pins = BTreeMap::new();
        add_pin(&mut pins, &index, None, boundary, observed, false).unwrap();
        add_pin(&mut pins, &index, None, boundary, plain, true).unwrap();
        let pinned = pins.clone();

        // With the log gone, only a request that must read its frame can fail.
        let moved = dir.path().join("moved.log");
        std::fs::rename(&path, &moved).unwrap();
        for (receipt, reducer) in [(plain, true), (plain, false), (observed, false)] {
            add_pin(&mut pins, &index, None, boundary, receipt, reducer).unwrap();
        }
        assert_eq!(pins, pinned);
        assert!(add_pin(&mut pins, &index, None, boundary, observed, true).is_err());
        assert_eq!(pins, pinned);

        std::fs::rename(&moved, &path).unwrap();
        let changed = AppendReceipt {
            sequence: observed.sequence,
            this_hash: plain.this_hash,
        };
        assert!(add_pin(&mut pins, &index, None, boundary, changed, false).is_err());
        assert_eq!(pins, pinned);
        let mut conflicting = pins.clone();
        conflicting.get_mut(&plain.sequence).unwrap().hash = observed.this_hash;
        let before = conflicting.clone();
        let error = add_pin(&mut conflicting, &index, None, boundary, plain, false).unwrap_err();
        assert_eq!(error.to_string(), "retention pins conflict");
        assert_eq!(conflicting, before);
        add_pin(&mut pins, &index, None, boundary, observed, true).unwrap();
        assert!(pins[&observed.sequence].reducer);
        assert_eq!(pins[&observed.sequence].wallet, Some(wallet));
    }

    #[test]
    fn pin_checks_erasure_against_the_supplied_authority() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source.log");
        let (receipts, index, offsets) = pin_fixture(
            &path,
            ["kept", "erased", "newer"]
                .map(|name| input(name, name.as_bytes().to_vec(), OLD, 1))
                .into(),
        );
        let (kept, erased, newer) = (receipts[0], receipts[1], receipts[2]);
        let old_boundary = boundary_at(&receipts, &offsets, 2);
        let boundary = boundary_at(&receipts, &offsets, 3);
        let expected = |position: usize| {
            Some(RetentionPin {
                sequence: receipts[position].sequence,
                offset: offsets[position],
                hash: receipts[position].this_hash,
                predecessor_hash: position
                    .checked_sub(1)
                    .map_or(blake3::Hash::from_bytes([0; 32]), |previous| {
                        receipts[previous].this_hash
                    }),
                reducer: false,
                wallet: None,
            })
        };
        let authority = RetentionAuthority {
            format_version: 1,
            epoch: 1,
            advanced_at: OLD,
            boundary: old_boundary,
            chain_head: erased.this_hash,
            pins: vec![expected(0).unwrap()],
            retained_tail: TailBinding::from(&index.current_tail_binding().unwrap()),
            feed: Vec::new(),
        };
        let pin = |receipt: AppendReceipt, current: Option<&RetentionAuthority>| {
            pin_for_receipt(&index, current, boundary, receipt, false)
        };
        assert_eq!(pin(kept, Some(&authority)).unwrap(), expected(0));
        assert_eq!(pin(newer, Some(&authority)).unwrap(), expected(2));
        assert_eq!(
            pin(erased, Some(&authority)).unwrap_err().to_string(),
            "retention input was already erased"
        );
        assert_eq!(pin(erased, None).unwrap(), expected(1));
    }

    /// #745. PASS: on a second advance the audit reads every record below the new boundary from
    /// the sidecar; a retired non-pin record with a broken checksum, the same record rewritten
    /// with a recomputed checksum, a differing window record and a truncated sidecar each block
    /// it, and the intact sidecar passes. FAIL: any damage accepted.
    #[test]
    fn retired_receipt_damage_blocks_the_next_advance_audit() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source.log");
        let (receipts, index, offsets) = pin_fixture(
            &path,
            (0..8)
                .map(|n| input("old", format!("{n}").into_bytes(), OLD + n, 1))
                .collect(),
        );
        let tail = index.current_tail_binding().unwrap();
        let frames = index.checkpoint_suffix(0, receipts.len(), &tail).unwrap();
        let sidecar = crate::source_checkpoint::receipts_path(&path);
        let intact = frames
            .iter()
            .flat_map(crate::source_checkpoint::encode_record)
            .collect::<Vec<_>>();
        std::fs::write(&sidecar, &intact).unwrap();
        index
            .install_retention(RetentionAuthority {
                format_version: 1,
                epoch: 1,
                advanced_at: OLD,
                boundary: boundary_at(&receipts, &offsets, 4),
                chain_head: receipts[3].this_hash,
                pins: vec![RetentionPin {
                    sequence: receipts[1].sequence,
                    offset: offsets[1],
                    hash: receipts[1].this_hash,
                    predecessor_hash: receipts[0].this_hash,
                    reducer: false,
                    wallet: None,
                }],
                retained_tail: TailBinding::from(&tail),
                feed: Vec::new(),
            })
            .unwrap();
        assert_eq!(index.retired_count(), 4);
        let second = boundary_at(&receipts, &offsets, 7);
        let audit = || index.retention_audit(second.sequence, &tail).unwrap();
        ensure_receipts(&path, &audit(), second).unwrap();

        let record = |position: usize| position * 80;
        let rewritten = |position: usize, bytes: &mut [u8]| {
            let mut frame = frames[position].clone();
            frame.received_millis += 1;
            bytes[record(position)..record(position + 1)]
                .copy_from_slice(&crate::source_checkpoint::encode_record(&frame));
        };
        let mut broken = intact.clone();
        broken[record(2) + 60] ^= 1;
        let mut recomputed = intact.clone();
        rewritten(2, &mut recomputed);
        let mut window = intact.clone();
        rewritten(5, &mut window);
        for (case, bytes, message) in [
            (
                "broken checksum",
                broken,
                "checkpoint receipt checksum mismatch",
            ),
            (
                "recomputed checksum",
                recomputed,
                "retention receipts differ from index",
            ),
            (
                "window record",
                window,
                "retention receipts differ from index",
            ),
            (
                "truncated",
                intact[..record(6)].to_vec(),
                "failed to fill whole buffer",
            ),
        ] {
            std::fs::write(&sidecar, &bytes).unwrap();
            let error = ensure_receipts(&path, &audit(), second).unwrap_err();
            assert!(error.to_string().contains(message), "{case}: {error}");
        }
        std::fs::write(&sidecar, &intact).unwrap();
        ensure_receipts(&path, &audit(), second).unwrap();
    }
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
    current: Option<&RetentionAuthority>,
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
        add_pin(pins, index, current, boundary, receipt, true)?;
    }
    for receipt in activity.binding_commitments {
        add_pin(pins, index, current, boundary, receipt, true)?;
        for dependency in commitment_source_receipts(receipt, index)? {
            add_pin(pins, index, current, boundary, dependency, false)?;
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
                add_pin(
                    pins,
                    index,
                    current,
                    boundary,
                    boundary_candidate.receipt,
                    true,
                )?;
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
    held: &[AppendReceipt],
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
    let cutoff = now
        .checked_sub(SOURCE_RETENTION_BUFFER_SECS)
        .context("retention clock overflow")?;
    let cutoff_millis = cutoff
        .checked_mul(1000)
        .context("retention clock overflow")?;
    let first = match index.retention_boundary_candidate(&tail, cutoff_millis)? {
        BoundaryCandidate::Empty => {
            return Ok(PreparedRetention::Nothing {
                tail,
                boundary_current: true,
            });
        }
        BoundaryCandidate::Retired { base } => {
            // The selected receipt is below the index's window base, which never passes the
            // committed boundary loaded above, so the boundary cannot advance.
            ensure!(
                base <= old_boundary.sequence.0,
                "retention index retired past the committed boundary"
            );
            return Ok(PreparedRetention::Nothing {
                tail,
                boundary_current: true,
            });
        }
        BoundaryCandidate::Frame(first) => first,
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
    let current = authority.as_ref();
    let mut pins = BTreeMap::new();
    let mut decision_rows = DecisionPinCache::default();
    let decisions = decision_pins(context, index, cutoff, &mut decision_rows)?;
    for receipt in decisions.receipts {
        cancelled(cancel)?;
        add_pin(&mut pins, index, current, boundary, receipt, false)?;
    }
    let activation_sequence = frozen
        .activation
        .last_sequence
        .context("retention requires a recorded migration activation frame")?;
    add_pin(
        &mut pins,
        index,
        current,
        boundary,
        receipt_at(index, activation_sequence)?,
        false,
    )?;
    let routed = reducer_pins(frozen, context, index, current, boundary, &mut pins)?;
    // The running poller may still read what it holds (correlation, retirement, a verified read's
    // bindings) after its group is disposed and the reducer prunes it; keep those frames readable.
    for receipt in held {
        cancelled(cancel)?;
        add_pin(&mut pins, index, current, boundary, *receipt, false)?;
    }
    let mut payload_receipts: HashMap<String, Vec<AppendReceipt>> = HashMap::new();
    if let Some(authority) = &authority {
        for pin in &authority.pins {
            cancelled(cancel)?;
            let (envelope, _) = authority.verify_pin(path, pin)?;
            if routed_artifact(&envelope, &routed)? {
                add_pin(
                    &mut pins,
                    index,
                    current,
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
                    add_pin(&mut pins, index, current, boundary, receipt, true)?;
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
        current,
        boundary,
    )?;
    // Receipts must be authentic and durable before an authority can make them retention metadata.
    ensure_receipts(
        path,
        &index.retention_audit(boundary.sequence, &tail)?,
        boundary,
    )?;
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
            expected_generation: frozen.authority_generation,
            cutoff,
            payload_receipts,
            decision_rows,
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
    current: Option<&RetentionAuthority>,
    boundary: RetentionBoundary,
) -> anyhow::Result<()> {
    for hash in hashes {
        if let Some(receipts) = payload_receipts.get(hash) {
            for receipt in receipts {
                add_pin(pins, index, current, boundary, *receipt, false)?;
            }
        }
    }
    Ok(())
}

/// Read every receipt below `boundary` from the sidecar and compare it with the verified index:
/// retired receipts through their commitment, the rest one by one (#745).
fn ensure_receipts(
    path: &Path,
    audit: &RetentionAudit,
    boundary: RetentionBoundary,
) -> anyhow::Result<()> {
    let count = boundary.sequence.0;
    let base = audit.commitment.count();
    let file = File::open(super::receipts_path(path))?;
    let mut reader = std::io::BufReader::new(&file);
    let mut rebuilt = ReceiptCommitment::default();
    for sequence in 0..base {
        rebuilt.push(&super::read_record(
            &mut reader,
            usize::try_from(sequence)?,
        )?)?;
    }
    ensure!(
        rebuilt.matches(&audit.commitment),
        "retention receipts differ from index"
    );
    for frame in &audit.window {
        let record = super::read_record(&mut reader, usize::try_from(frame.receipt.sequence.0)?)?;
        ensure!(
            record.receipt == frame.receipt
                && record.received_millis == frame.received_millis
                && record.byte_offset == frame.byte_offset,
            "retention receipts differ from index"
        );
    }
    ensure!(
        base.checked_add(u64::try_from(audit.window.len())?)
            .is_some_and(|audited| audited >= count),
        "retention receipts incomplete"
    );
    file.sync_all()?;
    Ok(())
}

/// Applied inside the orchestrator's dispatch owner, never concurrently with seed creation.
pub(crate) fn commit(
    mut request: RetentionCommitRequest,
) -> anyhow::Result<RetentionCommitOutcome> {
    let started = std::time::Instant::now();
    let path = request.receipts.current_tail_binding()?.path;
    let _lock = CheckpointLock::acquire(&path)?;
    // Only this commit writes the authority, under this lock, so one load serves every pin check below.
    let current = RetentionAuthority::load(&path)?;
    if current.as_ref().map_or(0, |authority| authority.epoch) != request.expected_epoch {
        return Ok(RetentionCommitOutcome::Deferred("epoch_changed"));
    }
    if let Some(reason) = pause_reason(&request.context)? {
        return Ok(RetentionCommitOutcome::Deferred(reason));
    }
    let checkpoint = super::read_authority(&path)?;
    ensure!(
        matches!(checkpoint, Authority::Readable(record) if record.retention_fence),
        "retention fence missing"
    );
    // An invalidation since the owner's capture may have trimmed the receipts this boundary needs.
    if !checkpoint.permits_checkpoint() || checkpoint.generation() != request.expected_generation {
        return Ok(RetentionCommitOutcome::Deferred("checkpoint_invalidated"));
    }
    let mut pins = request
        .authority
        .pins
        .iter()
        .cloned()
        .map(|pin| (pin.sequence, pin))
        .collect::<BTreeMap<_, _>>();
    let decisions = decision_pins(
        &request.context,
        &request.receipts,
        request.cutoff,
        &mut request.decision_rows,
    )?;
    let (decision_rows, decision_rows_derived) = (decisions.rows, decisions.derived);
    for receipt in decisions.receipts {
        add_pin(
            &mut pins,
            &request.receipts,
            current.as_ref(),
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
        current.as_ref(),
        request.authority.boundary,
    )?;
    request.authority.pins = pins.into_values().collect();
    // The authority is written and synced before the receipt-index lock is taken, and its
    // directory is synced after the lock is released, so other index users never wait on them (#754).
    let staged = request.authority.stage(&path)?;
    // The sidecar handle that serves retired receipts is opened under this lock, before the
    // authority can become visible; the prefix it reads is never rewritten afterwards (#745).
    let reader = request.receipts.retired_reader()?;
    let installer = request
        .receipts
        .prepare_retention_install(request.authority.clone(), Some(reader))?;
    let result = {
        #[cfg(feature = "scenario")]
        if let Some(publish) = &request.context.hooks.authority_write {
            publish(staged)
        } else {
            staged.publish()
        }
        #[cfg(not(feature = "scenario"))]
        staged.publish()
    };
    let published = match result {
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
    installer.install_retention();
    let directory_synced = published && sync_authority(&request.context, &path).is_ok();
    info!(
        epoch = request.authority.epoch,
        boundary_sequence = request.authority.boundary.sequence.0,
        boundary_offset = request.authority.boundary.offset,
        decision_rows,
        decision_rows_derived,
        elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
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
    receipts: &SourceReceiptIndex,
    tail: LogTailBinding,
    cancel: &AtomicBool,
) -> anyhow::Result<HashSet<WalletAddress>> {
    receipts.verify_retention_pins_cancellable(Some(cancel))?;
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
