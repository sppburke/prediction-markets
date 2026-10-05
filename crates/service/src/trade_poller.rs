//! Durable fixed-end Polymarket activity reconciliation (#544).
//!
//! Websocket observations and public polling pages are raw evidence. This owner
//! coalesces their wakeups per wallet, walks the existing paged/rate-gated REST
//! reader to a fixed end, and sends only complete epoch-second buckets to the
//! orchestrator's single [`crate::bucket_commit::BucketCommitEngine`] owner.
//! A websocket observation remains an obligation, derived from the source log
//! on restart, until its target has a durable terminal/apply record or its
//! wallet has a permanent fence and the observation cannot be bound to a target.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::FutureExt;

use pe_copy_signal_engine::{SignalConfig, TradeProvenance};
use pe_core_types::{
    MarketId, MarketOutcomeId, PolymarketTokenId, ReceivedAt, ReconstructionQuality, SourceId,
    SourceTimestamp, SourceTradeId, VenueMarketId, WalletAddress,
};
use pe_event_log::{AppendReceipt, ContentType, EnvelopeIn, EventEnvelope, Reader};
use pe_paper_state::{NoCopyDisposition, PaperStateDb};
use pe_position_ledger::LedgerEffect;
use pe_source_core::SourceError;
use pe_source_polymarket_public::{
    ACTIVITY_PARSER_VERSION, ACTIVITY_SCHEMA_VERSION, ActivityAggregate, ActivityReadError,
    ActivityType, NormalizedActivity, ReconciliationFetcher, fetch_complete_activity,
    parse_activity_trade_observation,
};
use pe_trader_index::WatchlistTier;
use time::OffsetDateTime;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::{AbortHandle, JoinSet};
use tracing::warn;

use crate::activity_ingest::{
    ACTIVITY_WS_SOURCE_ID, ReconciliationTrigger, SourceLogHandle, SourceLogHandleError,
};
use crate::asset_identity::{AssetIdentityResolver, IdentityProvenance};
use crate::bucket_commit::{
    ACTIVITY_READ_COMMITMENT_PARSER_VERSION, ACTIVITY_READ_COMMITMENT_SCHEMA_VERSION,
    ACTIVITY_READ_COMMITMENT_SOURCE_ID, BucketCommitResult, BucketDecisionContext,
    IdentityOverride, ObservationBinding, PageOccurrence, activity_read_commitment_payload_v2,
    joined_read_pages,
};
use crate::health::SharedHealth;
use crate::live_watchlist::LiveWatchlist;
use crate::orchestrator_control::OrchestratorControl;
use crate::risk_inputs::SourceReceiptIndex;
use crate::runtime_config::LiveRuntimeConfig;
use crate::watchlist_admission::{
    AdmissionPreparer, AnchorRefreshOutcome, RefreshHandoff, anchor_refresh_due,
};

/// Source id stamped on every fixed-end activity page before it is parsed.
pub const ACTIVITY_POLL_SOURCE_ID: &str = "polymarket-public.activity-reconciliation";
pub const DAILY_BOUNDARY_SOURCE_ID: &str = "pe-service.boundary";
/// Service-owned envelope schema of reconciliation pages written by a commitment-aware producer
/// (#565). The activity parser contract (`ACTIVITY_PARSER_VERSION`) is unchanged.
pub const ACTIVITY_POLL_PAGE_SCHEMA_VERSION: u32 = 3;
/// Best-effort cadence for refreshing venue-authoritative position anchors.
pub const ANCHOR_REFRESH_SECS: u64 = 3_600;
/// Compiled bound: one urgent, one backstop/boundary, and one refresh operation.
pub const TRADE_RECONCILIATION_CONCURRENCY: usize = 3;
const SECONDS_PER_DAY: i64 = 86_400;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum BoundaryScheduleError {
    #[error("daily boundary timestamp arithmetic overflow")]
    Overflow,
}

/// Every completed UTC midnight strictly after the latest boundary or Start, oldest first.
pub fn completed_midnight_cutoffs_after(
    anchor_unix: i64,
    now_unix: i64,
) -> Result<Vec<i64>, BoundaryScheduleError> {
    if now_unix <= anchor_unix {
        return Ok(Vec::new());
    }
    let anchor_day = anchor_unix.div_euclid(SECONDS_PER_DAY);
    let mut cutoff = anchor_day
        .checked_add(1)
        .and_then(|day| day.checked_mul(SECONDS_PER_DAY))
        .ok_or(BoundaryScheduleError::Overflow)?;
    let latest_completed = now_unix
        .div_euclid(SECONDS_PER_DAY)
        .checked_mul(SECONDS_PER_DAY)
        .ok_or(BoundaryScheduleError::Overflow)?;
    let mut cutoffs = Vec::new();
    while cutoff <= latest_completed {
        cutoffs.push(cutoff);
        if cutoff == latest_completed {
            break;
        }
        cutoff = cutoff
            .checked_add(SECONDS_PER_DAY)
            .ok_or(BoundaryScheduleError::Overflow)?;
    }
    Ok(cutoffs)
}

/// Configuration for the existing per-wallet polling cadence.
#[derive(Debug, Clone)]
pub struct TradePollerConfig {
    pub base_url: String,
    pub poll_interval_secs: u64,
    pub activity_ws_enabled: bool,
    pub copy_latency_budget_secs: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct Obligation {
    #[serde(default)]
    qualifying_buy: bool,
    group_id: SourceTradeId,
    received_at: OffsetDateTime,
    receipt: pe_event_log::AppendReceipt,
    bindings: Vec<ObservationBinding>,
    #[serde(default)]
    frame_admission_receipt: Option<AppendReceipt>,
    #[serde(default)]
    retained_commitments: Vec<AppendReceipt>,
}

type WalletObligations = BTreeMap<i64, BTreeMap<String, Obligation>>;
type CoalescedObligations = HashMap<WalletAddress, WalletObligations>;

fn insert_coalesced_obligation(
    by_wallet: &mut CoalescedObligations,
    wallet: WalletAddress,
    epoch: i64,
    obligation: Obligation,
) {
    let epochs = by_wallet.entry(wallet).or_default();
    let existing = epochs.iter().find_map(|(epoch, groups)| {
        groups
            .get(&obligation.group_id.0)
            .map(|existing| (*epoch, existing))
    });
    if let Some((previous_epoch, existing)) = existing {
        if !crate::frame_admission::prefer_observation(
            existing.receipt,
            existing.frame_admission_receipt.is_some(),
            existing.qualifying_buy,
            obligation.receipt,
            obligation.frame_admission_receipt.is_some(),
            obligation.qualifying_buy,
        ) {
            return;
        }
        if let Some(groups) = epochs.get_mut(&previous_epoch) {
            groups.remove(&obligation.group_id.0);
        }
        epochs.retain(|_, groups| !groups.is_empty());
    }
    epochs
        .entry(epoch)
        .or_default()
        .insert(obligation.group_id.0.clone(), obligation);
}

fn insert_reconciliation_trigger(
    by_wallet: &mut CoalescedObligations,
    trigger: ReconciliationTrigger,
) {
    let epoch = trigger.source_time.unix_timestamp();
    insert_coalesced_obligation(
        by_wallet,
        trigger.wallet,
        epoch,
        Obligation {
            qualifying_buy: trigger.qualifying_buy,
            frame_admission_receipt: None,
            retained_commitments: Vec::new(),
            group_id: trigger.source_trade_id,
            received_at: trigger.received_at,
            receipt: trigger.receipt,
            bindings: Vec::new(),
        },
    );
}

#[derive(Clone, Default)]
struct BucketIdentities {
    overrides: HashMap<SourceTradeId, IdentityOverride>,
    unresolved: HashSet<SourceTradeId>,
    provenance: BTreeMap<PolymarketTokenId, IdentityProvenance>,
}

/// Coalesced durable websocket work rebuilt from source evidence on restart.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReconciliationObligations {
    by_wallet: CoalescedObligations,
    boundary: Option<PendingBoundary>,
    last_boundary_cutoff: Option<i64>,
    routed_frames: HashSet<pe_core_types::EventSeq>,
    frame_candidates: HashMap<SourceTradeId, AppendReceipt>,
    retired_frame_receipts: Vec<AppendReceipt>,
    retired_frame_ids: HashSet<SourceTradeId>,
}

/// Log-pure websocket candidates awaiting one durable-state filter (#572).
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct ActivityCandidates {
    by_wallet: CoalescedObligations,
    binding_commitments: Vec<AppendReceipt>,
    #[serde(default)]
    routed_frames: HashSet<pe_core_types::EventSeq>,
    #[serde(default)]
    frame_candidates: HashMap<SourceTradeId, AppendReceipt>,
}

impl ActivityCandidates {
    /// Observe one source frame without consulting paper state (#572).
    pub(crate) fn observe_activity(
        &mut self,
        envelope: &EventEnvelope,
    ) -> Result<(), ObligationRebuildError> {
        if envelope.source_id.0 == crate::frame_admission::FRAME_FALLBACK_SOURCE_ID {
            let artifact: crate::frame_admission::FrameFallbackArtifact =
                serde_json::from_slice(&envelope.payload)
                    .map_err(|error| ObligationRebuildError::Binding(error.to_string()))?;
            if envelope.schema_version != 1
                || envelope.parser_version != 1
                || artifact.version != 1
                || artifact.frame_receipt.sequence >= envelope.seq
            {
                return Err(ObligationRebuildError::Binding(
                    "invalid frame fallback artifact".to_owned(),
                ));
            }
            self.routed_frames.insert(artifact.frame_receipt.sequence);
            return Ok(());
        }
        if envelope.source_id.0 == ACTIVITY_READ_COMMITMENT_SOURCE_ID {
            let commitment: crate::bucket_commit::ActivityReadCommitment =
                serde_json::from_slice(&envelope.payload)
                    .map_err(|error| ObligationRebuildError::Binding(error.to_string()))?;
            let generation = (
                envelope.schema_version,
                envelope.parser_version,
                commitment.version,
            );
            if !matches!(generation, (1, 1, 1) | (2, 1, 2))
                || (commitment.version == 2) != commitment.bindings.is_some()
                || (commitment.version == 1 && commitment.read_proof.is_some())
                || envelope.content_type != ContentType::Json
            {
                return Err(ObligationRebuildError::Binding(
                    "commitment generation differs".to_owned(),
                ));
            }
            if commitment
                .bindings
                .as_ref()
                .is_some_and(|bindings| !bindings.is_empty())
                || commitment.read_proof.is_some()
            {
                self.binding_commitments.push(AppendReceipt {
                    sequence: envelope.seq,
                    this_hash: envelope.this_hash,
                });
            }
            return Ok(());
        }
        if envelope.source_id.0 != ACTIVITY_WS_SOURCE_ID
            || envelope.schema_version != ACTIVITY_SCHEMA_VERSION
            || envelope.parser_version != ACTIVITY_PARSER_VERSION
        {
            return Ok(());
        }
        let activity = parse_activity_trade_observation(&envelope.payload)?;
        if activity.group_id.components().side == Some(pe_core_types::Side::Buy)
            && activity.share_amount != pe_core_types::ShareAmount::ZERO
            && !activity.is_combo
        {
            self.frame_candidates
                .entry(activity.group_id.key().clone())
                .or_insert(AppendReceipt {
                    sequence: envelope.seq,
                    this_hash: envelope.this_hash,
                });
        }
        insert_reconciliation_trigger(
            &mut self.by_wallet,
            ReconciliationTrigger {
                qualifying_buy: activity.group_id.components().side
                    == Some(pe_core_types::Side::Buy)
                    && activity.share_amount != pe_core_types::ShareAmount::ZERO
                    && !activity.is_combo,
                wallet: activity.wallet,
                source_time: activity.source_time.0,
                source_trade_id: activity.group_id.key().clone(),
                provenance: TradeProvenance::ActivityWs,
                received_at: envelope.received_at.0,
                receipt: AppendReceipt {
                    sequence: envelope.seq,
                    this_hash: envelope.this_hash,
                },
            },
        );
        Ok(())
    }

    /// Number of coalesced candidates.
    #[must_use]
    pub(crate) fn len(&self) -> usize {
        self.by_wallet
            .values()
            .flat_map(BTreeMap::values)
            .map(BTreeMap::len)
            .sum()
    }

    /// Filter the coalesced log candidates against durable paper state (#572). The result is a
    /// keyed map, so the visiting order does not affect it.
    pub(crate) fn into_obligations(
        self,
        paper_state: &PaperStateDb,
        source_receipts: &SourceReceiptIndex,
    ) -> Result<ReconciliationObligations, ObligationRebuildError> {
        let mut by_wallet = self.by_wallet;
        for (wallet, epoch, obligation) in admitted_frame_obligations(paper_state, None)? {
            let epochs = by_wallet.entry(wallet).or_default();
            replace_admitted_obligation(epochs, epoch, obligation, true);
        }
        let mut bindings = HashMap::<_, Vec<ObservationBinding>>::new();
        let mut retained = HashMap::<WalletAddress, Vec<AppendReceipt>>::new();
        let mut matched_frames = Vec::new();
        for receipt in self.binding_commitments {
            let read = crate::bucket_commit::verified_commitment_bindings(receipt, source_receipts)
                .map_err(|error| ObligationRebuildError::Binding(error.to_string()))?;
            retained.entry(read.wallet).or_default().push(receipt);
            for binding in &read.bindings {
                if let Some(frame) =
                    paper_state.activity_frame_decision(&binding.stream_group_id)?
                    && matches!(
                        crate::feed_audit::disposition(&frame, &read).map_err(|error| {
                            ObligationRebuildError::Binding(error.to_string())
                        })?,
                        crate::feed_audit::AuditDisposition::Matched(_)
                    )
                {
                    matched_frames.push(binding.stream_receipt);
                }
                bindings
                    .entry((
                        binding.stream_group_id.clone(),
                        binding.stream_receipt.sequence,
                        binding.stream_receipt.this_hash,
                    ))
                    .or_default()
                    .push(binding.clone());
            }
        }
        let mut obligations = ReconciliationObligations {
            routed_frames: self.routed_frames,
            frame_candidates: self.frame_candidates,
            ..ReconciliationObligations::default()
        };
        for (wallet, epochs) in by_wallet {
            let fenced = paper_state.is_wallet_fenced(&wallet)?;
            for (epoch, groups) in epochs {
                for mut obligation in groups.into_values() {
                    obligation.bindings = bindings
                        .remove(&(
                            obligation.group_id.clone(),
                            obligation.receipt.sequence,
                            obligation.receipt.this_hash,
                        ))
                        .unwrap_or_default();
                    if obligation.frame_admission_receipt.is_some() {
                        obligation.retained_commitments =
                            retained.get(&wallet).cloned().unwrap_or_default();
                    }
                    // A fence alone is not an acknowledgement. Keep ordinary fenced work
                    // until the serialized owner persists retirement and removes its barrier.
                    if (obligation.frame_admission_receipt.is_none()
                        && obligation.bindings.is_empty()
                        && fenced
                        && paper_state
                            .activity_group_state(&obligation.group_id)?
                            .is_none()
                        && !paper_state.activity_observation_unbound_retired(obligation.receipt)?)
                        || (obligation.frame_admission_receipt.is_some()
                            && !matched_frames.contains(&obligation.receipt))
                        || !obligation_disposed(
                            paper_state,
                            fenced,
                            &obligation,
                            matched_frames.contains(&obligation.receipt),
                        )?
                    {
                        insert_coalesced_obligation(
                            &mut obligations.by_wallet,
                            wallet,
                            epoch,
                            obligation,
                        );
                    } else if obligation.frame_admission_receipt.is_some() {
                        obligations.retired_frame_receipts.push(obligation.receipt);
                        obligations.retired_frame_ids.insert(obligation.group_id);
                    }
                }
            }
        }
        Ok(obligations)
    }
}

/// The sole source-ordered daily boundary waiting for qualifying activity acknowledgements.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PendingBoundary {
    pub cutoff_unix: i64,
    pub receipt: AppendReceipt,
}

impl ReconciliationObligations {
    /// Receipt-ordered unfinished frame work. Fallbacks stay reconciliation obligations but
    /// are not routed a second time at boot.
    #[must_use]
    pub fn frame_recovery_receipts(&self) -> (Vec<AppendReceipt>, Vec<AppendReceipt>) {
        let outstanding = self
            .by_wallet
            .values()
            .flat_map(BTreeMap::values)
            .flat_map(BTreeMap::values)
            .map(|obligation| obligation.group_id.clone())
            .collect::<HashSet<_>>();
        let mut all = self
            .frame_candidates
            .iter()
            .filter(|(id, _)| outstanding.contains(*id))
            .map(|(_, receipt)| *receipt)
            .collect::<Vec<_>>();
        // Wallet-age evidence and recovery include SELL/raw-only receipts and checkpoints written
        // before frame_candidates existed. Admission uses the authenticated payload to ignore
        // those that cannot qualify; a later ordinary BUY candidate retains its own first receipt.
        all.extend(
            self.by_wallet
                .values()
                .flat_map(BTreeMap::values)
                .flat_map(BTreeMap::values)
                .map(|obligation| obligation.receipt),
        );
        all.sort_by_key(|receipt| receipt.sequence);
        all.dedup();
        let undelivered = all
            .iter()
            .filter(|receipt| !self.routed_frames.contains(&receipt.sequence))
            .copied()
            .collect();
        (all, undelivered)
    }

    /// Retire synchronized incident receipts from the coalesced unresolved work.
    pub fn retire_feed_incidents(
        &mut self,
        era: &crate::paper_recovery::PaperEra,
        paper_state: &PaperStateDb,
    ) -> Result<(), pe_paper_state::PaperStateError> {
        let mut retired = crate::feed_audit::audited_receipts(era);
        for obligation in self
            .by_wallet
            .values()
            .flat_map(BTreeMap::values)
            .flat_map(BTreeMap::values)
        {
            if retired.contains(&obligation.receipt) {
                for binding in &obligation.bindings {
                    if !binding_target_disposed(paper_state, binding)? {
                        retired.retain(|receipt| *receipt != obligation.receipt);
                        break;
                    }
                }
            }
        }
        for receipt in &retired {
            if !self.retired_frame_receipts.contains(receipt) {
                self.retired_frame_receipts.push(*receipt);
            }
        }
        self.retired_frame_ids.extend(
            self.frame_candidates
                .iter()
                .filter(|(_, receipt)| retired.contains(receipt))
                .map(|(id, _)| id.clone()),
        );
        for epochs in self.by_wallet.values_mut() {
            for groups in epochs.values_mut() {
                self.retired_frame_ids.extend(
                    groups
                        .values()
                        .filter(|obligation| retired.contains(&obligation.receipt))
                        .map(|obligation| obligation.group_id.clone()),
                );
                groups.retain(|_, obligation| !retired.contains(&obligation.receipt));
            }
            epochs.retain(|_, groups| !groups.is_empty());
        }
        self.by_wallet.retain(|_, epochs| !epochs.is_empty());
        Ok(())
    }

    pub fn insert(&mut self, trigger: ReconciliationTrigger) {
        if !self.retired_frame_receipts.contains(&trigger.receipt)
            && !self.retired_frame_ids.contains(&trigger.source_trade_id)
        {
            insert_reconciliation_trigger(&mut self.by_wallet, trigger);
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_wallet.is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.by_wallet
            .values()
            .flat_map(BTreeMap::values)
            .map(BTreeMap::len)
            .sum()
    }

    #[cfg(feature = "scenario")]
    #[must_use]
    pub fn unresolved_receipts(&self, wallet: WalletAddress) -> Vec<AppendReceipt> {
        let mut receipts = self
            .by_wallet
            .get(&wallet)
            .into_iter()
            .flat_map(BTreeMap::values)
            .flat_map(BTreeMap::values)
            .map(|obligation| obligation.receipt)
            .collect::<Vec<_>>();
        receipts.sort_by_key(|receipt| receipt.sequence);
        receipts
    }

    /// Install one pending boundary. A second boundary never replaces the oldest.
    pub fn install_boundary(&mut self, boundary: PendingBoundary) -> bool {
        if self.boundary.is_some() {
            return false;
        }
        self.boundary = Some(boundary);
        true
    }

    pub fn set_boundary_anchor(&mut self, cutoff_unix: i64) {
        self.last_boundary_cutoff = Some(cutoff_unix);
    }

    #[must_use]
    pub fn boundary_anchor(&self) -> Option<i64> {
        self.last_boundary_cutoff
    }

    /// The oldest pending boundary, if any.
    #[must_use]
    pub fn pending_boundary(&self) -> Option<PendingBoundary> {
        self.boundary
    }

    /// True after every obligation inside both the receipt and receive-time bounds is gone.
    #[must_use]
    pub fn boundary_ready(&self) -> bool {
        let Some(boundary) = self.boundary else {
            return false;
        };
        !self.by_wallet.values().any(|epochs| {
            epochs.values().any(|groups| {
                groups.values().any(|obligation| {
                    obligation.receipt.sequence <= boundary.receipt.sequence
                        && obligation.received_at.unix_timestamp() < boundary.cutoff_unix
                })
            })
        })
    }

    /// Remove and return the boundary only after its qualifying obligations are acknowledged.
    pub fn take_ready_boundary(&mut self) -> Option<PendingBoundary> {
        if self.boundary_ready() {
            self.boundary.take()
        } else {
            None
        }
    }

    /// Stable activation census persisted with the paper migration record.
    #[must_use]
    pub fn migration_evidence(&self) -> serde_json::Value {
        let mut rows = self
            .by_wallet
            .iter()
            .flat_map(|(wallet, epochs)| {
                epochs.iter().flat_map(move |(epoch, groups)| {
                    groups.values().map(move |obligation| {
                        serde_json::json!({
                            "wallet": wallet.to_string(),
                            "source_epoch": epoch,
                            "source_trade_id": obligation.group_id.0,
                            "received_at_unix": obligation.received_at.unix_timestamp(),
                            "receipt": obligation.receipt,
                        })
                    })
                })
            })
            .collect::<Vec<_>>();
        rows.sort_by_key(|row| row.to_string());
        serde_json::Value::Array(rows)
    }

    fn wallets(&self) -> impl Iterator<Item = WalletAddress> + '_ {
        self.by_wallet.keys().copied()
    }

    #[cfg(test)]
    fn earliest_epoch(&self, wallet: &WalletAddress) -> Option<i64> {
        self.by_wallet
            .get(wallet)
            .and_then(|epochs| epochs.first_key_value().map(|(epoch, _)| *epoch))
    }

    fn observation(
        &self,
        wallet: &WalletAddress,
        epoch: i64,
        group: &SourceTradeId,
    ) -> Option<(AppendReceipt, OffsetDateTime)> {
        self.by_wallet
            .get(wallet)?
            .get(&epoch)?
            .get(&group.0)
            .map(|obligation| (obligation.receipt, obligation.received_at))
    }

    fn remove_selected(&mut self, wallet: WalletAddress, epoch: i64, selected: &Obligation) {
        if selected.frame_admission_receipt.is_some() {
            self.retired_frame_ids.insert(selected.group_id.clone());
            if let Some(epochs) = self.by_wallet.get_mut(&wallet) {
                for groups in epochs.values_mut() {
                    groups.remove(&selected.group_id.0);
                }
                epochs.retain(|_, groups| !groups.is_empty());
                if epochs.is_empty() {
                    self.by_wallet.remove(&wallet);
                }
            }
        } else if self
            .observation(&wallet, epoch, &selected.group_id)
            .is_some_and(|(receipt, _)| receipt == selected.receipt)
        {
            self.remove(&wallet, epoch, &selected.group_id);
        }
    }

    fn remove(&mut self, wallet: &WalletAddress, epoch: i64, group: &SourceTradeId) {
        let mut remove_wallet = false;
        if let Some(epochs) = self.by_wallet.get_mut(wallet) {
            if let Some(groups) = epochs.get_mut(&epoch) {
                groups.remove(&group.0);
                if groups.is_empty() {
                    epochs.remove(&epoch);
                }
            }
            remove_wallet = epochs.is_empty();
        }
        if remove_wallet {
            self.by_wallet.remove(wallet);
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ObligationRebuildError {
    #[error("source-log replay: {0}")]
    Log(#[from] pe_event_log::LogError),
    #[error("source-log websocket observation: {0}")]
    Activity(#[from] pe_source_polymarket_public::ActivityParseError),
    #[error("paper-state: {0}")]
    PaperState(#[from] pe_paper_state::PaperStateError),
    #[error("paper-log boundary replay: {0}")]
    PaperLog(String),
    #[error("invalid daily-boundary source frame: {0}")]
    Boundary(String),
    #[error("source-log observation binding: {0}")]
    Binding(String),
}

/// Log-pure daily-boundary candidates awaiting the paper-log anchor (#572).
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct DailyBoundaryCandidates {
    boundaries: Vec<PendingBoundary>,
}

impl DailyBoundaryCandidates {
    /// Observe and validate one matching source frame without reading the paper log (#572).
    pub(crate) fn observe_daily_boundary(
        &mut self,
        envelope: &EventEnvelope,
    ) -> Result<(), ObligationRebuildError> {
        if envelope.source_id.0 != DAILY_BOUNDARY_SOURCE_ID {
            return Ok(());
        }
        let value: serde_json::Value = serde_json::from_slice(&envelope.payload)
            .map_err(|error| ObligationRebuildError::Boundary(error.to_string()))?;
        if value.get("kind").and_then(serde_json::Value::as_str) != Some("daily_boundary") {
            return Err(ObligationRebuildError::Boundary(
                "boundary source id carries an unexpected kind".to_owned(),
            ));
        }
        let cutoff_unix = value
            .get("cutoff_unix")
            .and_then(serde_json::Value::as_i64)
            .ok_or_else(|| {
                ObligationRebuildError::Boundary("boundary cutoff is absent".to_owned())
            })?;
        self.boundaries.push(PendingBoundary {
            cutoff_unix,
            receipt: AppendReceipt {
                sequence: envelope.seq,
                this_hash: envelope.this_hash,
            },
        });
        Ok(())
    }
}

/// Compute and install the Start/latest-mark anchor before source observation (#572).
pub(crate) fn recover_daily_boundary_anchor(
    paper_log_path: &Path,
    obligations: &mut ReconciliationObligations,
    paper_state: &PaperStateDb,
) -> Result<Option<i64>, ObligationRebuildError> {
    let frames = crate::paper_recovery::scan_paper_log(paper_log_path)
        .map_err(|error| ObligationRebuildError::PaperLog(error.to_string()))?;
    let era = crate::paper_recovery::paper_era(frames);
    obligations.retire_feed_incidents(&era, paper_state)?;
    recover_daily_boundary_anchor_from_era(&era, obligations)
}

pub(crate) fn recover_daily_boundary_anchor_from_era(
    era: &crate::paper_recovery::PaperEra,
    obligations: &mut ReconciliationObligations,
) -> Result<Option<i64>, ObligationRebuildError> {
    let Some((_, start)) = era.start.as_ref() else {
        return Ok(None);
    };
    let start_unix = era
        .frames
        .iter()
        .find_map(|frame| match &frame.frame {
            crate::paper_recovery::PaperLogFrame::Record(
                crate::paper_recovery::PaperLogRecord::QualificationStarted(candidate),
            ) if candidate.as_ref() == start.as_ref() => {
                Some(frame.envelope.received_at.0.unix_timestamp())
            }
            _ => None,
        })
        .ok_or_else(|| ObligationRebuildError::Boundary("Start envelope is absent".to_owned()))?;
    let anchor = era
        .frames
        .iter()
        .filter_map(|frame| match &frame.frame {
            crate::paper_recovery::PaperLogFrame::Record(
                crate::paper_recovery::PaperLogRecord::PortfolioMark(mark),
            ) => Some(mark.cutoff_unix),
            _ => None,
        })
        .max()
        .unwrap_or(start_unix);
    obligations.set_boundary_anchor(anchor);
    Ok(Some(anchor))
}

/// Select and install the oldest source candidate after the computed paper anchor (#572).
pub(crate) fn recover_daily_boundary_from_candidates(
    candidates: DailyBoundaryCandidates,
    anchor: i64,
    obligations: &mut ReconciliationObligations,
) {
    if let Some(boundary) = candidates
        .boundaries
        .into_iter()
        .filter(|boundary| boundary.cutoff_unix > anchor)
        .min_by_key(|boundary| (boundary.cutoff_unix, boundary.receipt.sequence))
    {
        obligations.install_boundary(boundary);
    }
}

/// Recover the oldest unacknowledged source boundary and the Start/latest-mark anchor.
pub fn recover_daily_boundary(
    source_log_path: &Path,
    paper_log_path: &Path,
    obligations: &mut ReconciliationObligations,
    paper_state: &PaperStateDb,
) -> Result<(), ObligationRebuildError> {
    let Some(anchor) = recover_daily_boundary_anchor(paper_log_path, obligations, paper_state)?
    else {
        return Ok(());
    };
    let mut candidates = DailyBoundaryCandidates::default();
    for item in Reader::replay(source_log_path)? {
        let (_sequence, envelope) = item?;
        candidates.observe_daily_boundary(&envelope)?;
    }
    recover_daily_boundary_from_candidates(candidates, anchor, obligations);
    Ok(())
}

/// Rebuild unresolved websocket obligations before any producer starts.
pub fn rebuild_reconciliation_obligations(
    source_log_path: &Path,
    paper_state: &PaperStateDb,
) -> Result<ReconciliationObligations, ObligationRebuildError> {
    rebuild_reconciliation_obligations_indexed(source_log_path, paper_state, None)
}

pub fn rebuild_reconciliation_obligations_with_index(
    source_log_path: &Path,
    paper_state: &PaperStateDb,
    index: &SourceReceiptIndex,
) -> Result<ReconciliationObligations, ObligationRebuildError> {
    rebuild_reconciliation_obligations_indexed(source_log_path, paper_state, Some(index))
}

fn rebuild_reconciliation_obligations_indexed(
    source_log_path: &Path,
    paper_state: &PaperStateDb,
    source_receipts: Option<&SourceReceiptIndex>,
) -> Result<ReconciliationObligations, ObligationRebuildError> {
    let mut candidates = ActivityCandidates::default();
    if let Some(index) = source_receipts {
        for item in Reader::replay(source_log_path)? {
            let (_, envelope) = item?;
            candidates.observe_activity(&envelope)?;
        }
        return candidates.into_obligations(paper_state, index);
    }
    let mut staging = SourceReceiptIndex::staging(source_log_path)
        .map_err(|error| ObligationRebuildError::Binding(error.to_string()))?;
    let mut last_sequence = None;
    let mut last_hash = blake3::Hash::from_bytes([0; 32]);
    for item in Reader::replay_with_offsets(source_log_path)? {
        let (offset, sequence, envelope) = item?;
        staging
            .observe(offset, &envelope)
            .map_err(|error| ObligationRebuildError::Binding(error.to_string()))?;
        last_sequence = Some(sequence);
        last_hash = envelope.this_hash;
        candidates.observe_activity(&envelope)?;
    }
    let binding = pe_event_log::LogTailBinding {
        path: std::fs::canonicalize(source_log_path)
            .map_err(|error| ObligationRebuildError::Binding(error.to_string()))?,
        physical_tail: std::fs::metadata(source_log_path)
            .map_err(|error| ObligationRebuildError::Binding(error.to_string()))?
            .len(),
        last_sequence,
        last_hash,
    };
    let index = staging
        .complete(&binding)
        .map_err(|error| ObligationRebuildError::Binding(error.to_string()))?;
    candidates.into_obligations(paper_state, &index)
}

#[derive(Debug, thiserror::Error)]
enum ReconciliationError {
    #[error("backstop visit yielded to a newer websocket obligation")]
    Preempted,
    #[error("activity reconciliation: {0}")]
    Activity(#[from] ActivityReadError),
    #[cfg(feature = "scenario")]
    #[error("injected reconciliation crash after {0:?}")]
    InjectedCrash(ReconciliationCrashBoundary),
    #[error("asset identity resolution: {0}")]
    Identity(SourceError),
    #[error("source-log coordinator closed")]
    SourceLogClosed,
    #[error("paper-state: {0}")]
    PaperState(#[from] pe_paper_state::PaperStateError),
    #[error("orchestrator control channel closed")]
    ControlClosed,
    #[error("orchestrator bucket commit failed: {0}")]
    BucketCommit(String),
    #[error("invalid reconstruction quality")]
    ReconstructionQuality,
    #[error("decision evidence json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("activity page receipts do not match the complete read")]
    PageReceiptMismatch,
    #[error("recorded observation binding: {0}")]
    Binding(String),
}

impl ReconciliationError {
    fn retryable(&self) -> bool {
        matches!(self, Self::Activity(_))
            || matches!(
                self,
                Self::Identity(SourceError::Transient { .. } | SourceError::RateLimited { .. })
            )
    }
}

#[cfg(feature = "scenario")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconciliationCrashBoundary {
    Commitment,
    Bucket,
}

/// Existing polling actor, now a complete fixed-end reconciliation coordinator.
pub struct TradePoller {
    config: TradePollerConfig,
    live_watchlist: LiveWatchlist,
    fetcher: Arc<dyn ReconciliationFetcher>,
    asset_identity: Arc<AssetIdentityResolver>,
    source_log: SourceLogHandle,
    trigger_rx: mpsc::Receiver<ReconciliationTrigger>,
    control_tx: mpsc::Sender<OrchestratorControl>,
    paper_state: Arc<PaperStateDb>,
    health: SharedHealth,

    signal_config: SignalConfig,
    runtime_config: LiveRuntimeConfig,
    obligations: ReconciliationObligations,
    admission_preparer: Option<AdmissionPreparer>,
    refresh_cursor: usize,
    refresh_reanchor_turn: bool,
    refresh_cooldown: HashMap<WalletAddress, tokio::time::Instant>,
    now: Arc<dyn Fn() -> OffsetDateTime + Send + Sync>,
    source_receipts: Option<SourceReceiptIndex>,
    #[cfg(feature = "scenario")]
    crash_boundary: Option<Arc<Mutex<Option<ReconciliationCrashBoundary>>>>,
    #[cfg(feature = "scenario")]
    progress: Option<mpsc::Sender<PollerProgress>>,
    #[cfg(feature = "scenario")]
    wait_observer: Option<mpsc::Sender<PollerWait>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AnchorRefreshClass {
    ReanchorRequired,
    AgeDue,
}

#[derive(Debug, thiserror::Error)]
pub enum TradePollerOwnerError {
    #[error("fixed-end reconciliation owner failed: {0}")]
    Reconciliation(String),
    #[error("activity reconciliation trigger channel closed")]
    TriggerChannelClosed,
    #[error("trade poll interval is zero")]
    ZeroPollInterval,
    #[error("position anchor refresh failed: {0}")]
    AnchorRefresh(String),
    #[error("daily boundary owner failed: {0}")]
    DailyBoundary(String),
}

#[derive(Clone, Default)]
struct WalletAttempt {
    selected: WalletObligations,
    deadline: Option<OffsetDateTime>,
    last_fixed_end: Option<i64>,
}

impl WalletAttempt {
    fn ready(&self, now: OffsetDateTime) -> bool {
        self.last_fixed_end.is_none() || self.deadline.is_some_and(|deadline| now <= deadline)
    }
}

#[derive(PartialEq, Eq)]
enum RoundStage {
    Boundary,
    Wallets,
    Publish,
    Done,
}

struct BackstopRound {
    wallets: VecDeque<WalletAddress>,
    live_wallets: Vec<WalletAddress>,
    stage: RoundStage,
    successes: usize,
    failures: usize,
}

impl BackstopRound {
    fn new(poller: &TradePoller) -> Self {
        let mut live_wallets = poller
            .live_watchlist
            .snapshot()
            .entries
            .iter()
            .map(|entry| entry.wallet)
            .collect::<Vec<_>>();
        live_wallets.sort_by_key(ToString::to_string);
        live_wallets.dedup();
        let mut wallets = live_wallets.clone();
        wallets.extend(poller.obligations.wallets());
        wallets.sort_by_key(ToString::to_string);
        wallets.dedup();
        Self {
            wallets: wallets.into(),
            live_wallets,
            stage: RoundStage::Boundary,
            successes: 0,
            failures: 0,
        }
    }
}

enum Completion {
    Reconciled {
        wallet: WalletAddress,
        urgent: bool,
        selected: WalletObligations,
        acknowledged_audits: Vec<(i64, Obligation)>,
        result: Result<Vec<(i64, Obligation)>, ReconciliationError>,
    },
    Refreshed(
        WalletAddress,
        Result<(AnchorRefreshOutcome, bool), TradePollerOwnerError>,
        tokio::time::Instant,
    ),
    BoundaryInstalled(Result<PendingBoundary, TradePollerOwnerError>),
    BoundaryPublished(PendingBoundary, Result<(), TradePollerOwnerError>),
}

/// Bounded scenario-only completion barriers; production has no observer channel.
#[cfg(feature = "scenario")]
#[derive(Debug)]
pub enum PollerProgress {
    Started {
        wallet: WalletAddress,
        urgent: bool,
        frontier: Vec<AppendReceipt>,
    },
    Completed {
        wallet: WalletAddress,
        selected: Vec<AppendReceipt>,
        unresolved: Vec<AppendReceipt>,
    },
    RoundCompleted,
}

/// Scenario-only snapshot taken immediately before the coordinator waits for progress.
#[cfg(feature = "scenario")]
pub struct PollerWait {
    pub obligations: ReconciliationObligations,
    pub wake: Option<tokio::time::Instant>,
    pub refresh_cooldown: HashMap<WalletAddress, tokio::time::Instant>,
}

fn remove_wallet_obligation(selected: &mut WalletObligations, epoch: i64, group: &SourceTradeId) {
    if let Some(groups) = selected.get_mut(&epoch) {
        groups.remove(&group.0);
        if groups.is_empty() {
            selected.remove(&epoch);
        }
    }
}

impl TradePoller {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: TradePollerConfig,
        live_watchlist: LiveWatchlist,
        fetcher: Arc<dyn ReconciliationFetcher>,
        asset_identity: Arc<AssetIdentityResolver>,
        source_log: SourceLogHandle,
        trigger_rx: mpsc::Receiver<ReconciliationTrigger>,
        control_tx: mpsc::Sender<OrchestratorControl>,
        paper_state: Arc<PaperStateDb>,
        health: SharedHealth,

        signal_config: SignalConfig,
        runtime_config: LiveRuntimeConfig,
        obligations: ReconciliationObligations,
        admission_preparer: Option<AdmissionPreparer>,
    ) -> Self {
        Self {
            config,
            live_watchlist,
            fetcher,
            asset_identity,
            source_log,
            trigger_rx,
            control_tx,
            paper_state,
            health,
            signal_config,
            runtime_config,
            obligations,
            admission_preparer,
            refresh_cursor: 0,
            refresh_reanchor_turn: true,
            refresh_cooldown: HashMap::new(),
            now: Arc::new(OffsetDateTime::now_utc),
            source_receipts: None,
            #[cfg(feature = "scenario")]
            crash_boundary: None,
            #[cfg(feature = "scenario")]
            progress: None,
            #[cfg(feature = "scenario")]
            wait_observer: None,
        }
    }

    #[cfg(feature = "scenario")]
    pub fn with_crash_boundary(
        mut self,
        boundary: Arc<Mutex<Option<ReconciliationCrashBoundary>>>,
    ) -> Self {
        self.crash_boundary = Some(boundary);
        self
    }

    /// Deterministic reconciliation clock for hermetic scenarios.
    #[cfg(feature = "scenario")]
    pub fn with_clock(mut self, now: Arc<dyn Fn() -> OffsetDateTime + Send + Sync>) -> Self {
        self.now = now;
        self
    }

    /// Bounded, best-effort scheduling snapshots for deterministic scenarios.
    #[cfg(feature = "scenario")]
    pub fn with_wait_observer(mut self, observer: mpsc::Sender<PollerWait>) -> Self {
        self.wait_observer = Some(observer);
        self
    }

    /// Share the coordinator's existing receipt index for raw observation and metadata lookup.
    #[must_use]
    pub fn with_source_receipt_index(mut self, index: SourceReceiptIndex) -> Self {
        self.source_receipts = Some(index);
        self
    }

    /// Explicit operation barriers for deterministic scheduler scenarios.
    #[cfg(feature = "scenario")]
    #[must_use]
    pub fn with_progress(mut self, progress: mpsc::Sender<PollerProgress>) -> Self {
        self.progress = Some(progress);
        self
    }

    pub async fn run(self) {
        let _ = self.run_until(std::future::pending::<()>()).await;
    }

    /// Receive triggers throughout reads. Shutdown cancels venue work, but a sent bucket
    /// commit or anchor install retains wallet ownership until its acknowledgement or timeout.
    /// Accepted source pages, commitments, and buckets remain durable; an obligation whose
    /// target was not disposed is rebuilt from the source log at the next boot. A refresh not
    /// handed over is re-derived from the anchor at the next boot. A failed operation stops
    /// admission after remaining started operations finish.
    pub async fn run_until(
        mut self,
        shutdown: impl Future<Output = ()>,
    ) -> Result<(), TradePollerOwnerError> {
        if self.config.poll_interval_secs == 0 {
            return Err(TradePollerOwnerError::ZeroPollInterval);
        }
        self.health
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .poll_started_at = Some((self.now)());
        tokio::pin!(shutdown);
        let mut tasks = JoinSet::new();
        let mut busy_wallets = HashSet::new();
        let mut urgent_busy = false;
        let mut backstop_busy = false;
        let mut refresh_busy = false;
        let mut refresh_pending = None;
        let mut refresh_visit: Option<(WalletAddress, RefreshHandoff, AbortHandle)> = None;
        let mut backstop_visit: Option<(WalletAddress, watch::Sender<bool>)> = None;
        let mut urgent_visit: Option<watch::Sender<bool>> = None;
        // Expiry changes readiness, not ownership: the frozen frontier returns to the backstop
        // until disposed, while later receipts remain queued for a subsequent attempt.
        let mut attempts = HashMap::<WalletAddress, WalletAttempt>::new();
        let mut last_launch = HashMap::<WalletAddress, tokio::time::Instant>::new();
        let mut refresh_reconcile = HashMap::<WalletAddress, Option<i64>>::new();
        let mut refresh_retry = VecDeque::new();
        let mut round = BackstopRound::new(&self);
        let mut cadence = tokio::time::Instant::now();
        let mut stopping = false;
        let mut shutdown_requested = false;
        let mut failure = None;
        loop {
            // Poll shutdown before admitting work, including at startup.
            // A stop seen here (between two select polls) cancels started operations exactly
            // like the select arm below, and a stop that arrives after a failure-initiated stop
            // still cancels whatever is left.
            if !shutdown_requested && shutdown.as_mut().now_or_never().is_some() {
                shutdown_requested = true;
                stopping = true;
                if let Some((_, cancel)) = &backstop_visit {
                    let _ = cancel.send(true);
                }
                if let Some(cancel) = &urgent_visit {
                    let _ = cancel.send(true);
                }
                if let Some((_, handoff, handle)) = &refresh_visit
                    && handoff.cancel_before_handoff()
                {
                    handle.abort();
                }
            }
            if !stopping {
                self.drain_triggers();
                let now = (self.now)();
                if let Some((wallet, handoff, handle)) = &refresh_visit
                    && self.obligations.by_wallet.contains_key(wallet)
                    && handoff.cancel_before_handoff()
                {
                    handle.abort();
                }
                if let Some((wallet, cancel)) = &backstop_visit
                    && self.has_new_obligation(*wallet, &attempts)
                {
                    let _ = cancel.send(true);
                }
                if !stopping && !urgent_busy {
                    let mut wallets = self
                        .obligations
                        .wallets()
                        .chain(refresh_reconcile.keys().copied())
                        .collect::<Vec<_>>();
                    wallets.sort_by_key(|wallet| {
                        (last_launch.get(wallet).copied(), wallet.to_string())
                    });
                    wallets.dedup();
                    for wallet in wallets {
                        if busy_wallets.contains(&wallet) {
                            continue;
                        }
                        let forced = refresh_reconcile
                            .get(&wallet)
                            .is_some_and(|end| end.is_none_or(|end| now.unix_timestamp() > end));
                        let ready = if let Some(attempt) = attempts.get(&wallet) {
                            attempt.ready(now)
                        } else {
                            self.freeze_attempt(wallet, true).is_some()
                        };
                        if !forced && !ready {
                            continue;
                        }
                        let attempt = attempts.entry(wallet).or_insert_with(|| {
                            self.freeze_attempt(wallet, !forced).unwrap_or_default()
                        });
                        attempt.last_fixed_end = Some(now.unix_timestamp());
                        last_launch.insert(wallet, tokio::time::Instant::now());
                        if forced {
                            refresh_reconcile.insert(wallet, Some(now.unix_timestamp()));
                        }
                        let (cancel, receiver) = watch::channel(false);
                        self.spawn_reconciliation(
                            &mut tasks,
                            wallet,
                            attempt,
                            true,
                            now.unix_timestamp(),
                            Some(receiver),
                        );
                        urgent_visit = Some(cancel);
                        busy_wallets.insert(wallet);
                        urgent_busy = true;
                        break;
                    }
                }
                if !stopping && !backstop_busy {
                    if round.stage == RoundStage::Done && tokio::time::Instant::now() >= cadence {
                        round = BackstopRound::new(&self);
                        // An operation already admitted in the urgent slot is this wallet's
                        // current visit; it must not hold the background round at its barrier.
                        round
                            .wallets
                            .retain(|wallet| !busy_wallets.contains(wallet));
                    }
                    let operation = self.operation();
                    match round.stage {
                        RoundStage::Boundary => {
                            round.stage = RoundStage::Wallets;
                            if self.obligations.pending_boundary().is_none()
                                && let Some(anchor) = self.obligations.boundary_anchor()
                            {
                                match completed_midnight_cutoffs_after(anchor, now.unix_timestamp())
                                {
                                    Ok(cutoffs) => {
                                        if let Some(cutoff) = cutoffs.into_iter().next() {
                                            tasks.spawn(async move {
                                                Completion::BoundaryInstalled(
                                                    operation.install_boundary(cutoff).await,
                                                )
                                            });
                                            backstop_busy = true;
                                        }
                                    }
                                    Err(error) => {
                                        failure = Some(TradePollerOwnerError::DailyBoundary(
                                            error.to_string(),
                                        ));
                                        stopping = true;
                                    }
                                }
                            }
                        }
                        RoundStage::Wallets => {
                            // A refresh owns this wallet's anchor handoff. It has not been
                            // visited for this round, so retry the visit next round instead of
                            // waiting here or counting it as healthy.
                            if let Some((wallet, _, _)) = &refresh_visit {
                                round.wallets.retain(|candidate| candidate != wallet);
                            }
                            if let Some(index) = round
                                .wallets
                                .iter()
                                .position(|wallet| !busy_wallets.contains(wallet))
                            {
                                if let Some(wallet) = round.wallets.remove(index) {
                                    if attempts.get(&wallet).is_some_and(|attempt| {
                                        !attempt.selected.is_empty()
                                            && attempt
                                                .deadline
                                                .is_some_and(|deadline| now <= deadline)
                                            && attempt
                                                .last_fixed_end
                                                .is_some_and(|end| now.unix_timestamp() <= end)
                                    }) {
                                        continue;
                                    }
                                    let attempt = attempts.entry(wallet).or_insert_with(|| {
                                        self.freeze_attempt(wallet, false).unwrap_or_default()
                                    });
                                    attempt.last_fixed_end = Some(now.unix_timestamp());
                                    last_launch.insert(wallet, tokio::time::Instant::now());
                                    let (cancel, receiver) = watch::channel(false);
                                    self.spawn_reconciliation(
                                        &mut tasks,
                                        wallet,
                                        attempt,
                                        false,
                                        now.unix_timestamp(),
                                        Some(receiver),
                                    );
                                    backstop_visit = Some((wallet, cancel));
                                    busy_wallets.insert(wallet);
                                    backstop_busy = true;
                                }
                            } else if round.wallets.is_empty() {
                                round.stage = RoundStage::Publish;
                            }
                        }
                        RoundStage::Publish => {
                            round.stage = RoundStage::Done;
                            self.record_round_health(&round);
                            if refresh_pending.is_none() {
                                match self.select_refresh_wallet(&round.live_wallets) {
                                    Ok(wallet) => refresh_pending = wallet,
                                    Err(error) => {
                                        failure = Some(error);
                                        stopping = true;
                                    }
                                }
                            }
                            cadence = tokio::time::Instant::now()
                                + Duration::from_secs(self.config.poll_interval_secs);
                            #[cfg(feature = "scenario")]
                            self.report_progress(PollerProgress::RoundCompleted);
                            if let Some(boundary) = self.obligations.take_ready_boundary() {
                                tasks.spawn(async move {
                                    Completion::BoundaryPublished(
                                        boundary,
                                        operation.publish_boundary(boundary).await,
                                    )
                                });
                                backstop_busy = true;
                            }
                        }
                        RoundStage::Done => {
                            if let Some(boundary) = self.obligations.take_ready_boundary() {
                                tasks.spawn(async move {
                                    Completion::BoundaryPublished(
                                        boundary,
                                        operation.publish_boundary(boundary).await,
                                    )
                                });
                                backstop_busy = true;
                            }
                        }
                    }
                }
                if !stopping && !refresh_busy {
                    let retry = refresh_retry.iter().position(|wallet| {
                        !busy_wallets.contains(wallet)
                            && !self.obligations.by_wallet.contains_key(wallet)
                            && self
                                .refresh_cooldown
                                .get(wallet)
                                .is_none_or(|deadline| tokio::time::Instant::now() >= *deadline)
                    });
                    let selected =
                        retry
                            .and_then(|index| refresh_retry.remove(index))
                            .or_else(|| {
                                refresh_pending.filter(|wallet| {
                                    !busy_wallets.contains(wallet)
                                        && !self.obligations.by_wallet.contains_key(wallet)
                                        && self.refresh_cooldown.get(wallet).is_none_or(
                                            |deadline| tokio::time::Instant::now() >= *deadline,
                                        )
                                })
                            });
                    if let Some(wallet) = selected {
                        if refresh_pending == Some(wallet) {
                            refresh_pending = None;
                        }
                        let handoff = RefreshHandoff::default();
                        let observed = handoff.clone();
                        let operation = self.operation();
                        let handle = tasks.spawn(async move {
                            let result = operation.refresh_one_wallet(wallet, &observed).await;
                            let completed_at = tokio::time::Instant::now();
                            Completion::Refreshed(wallet, result, completed_at)
                        });
                        busy_wallets.insert(wallet);
                        refresh_busy = true;
                        refresh_visit = Some((wallet, handoff, handle));
                    }
                }
            }
            if stopping && tasks.is_empty() {
                break;
            }
            // Synchronous stage transitions must finish without waiting for a timer or trigger.
            let background_ready = match round.stage {
                RoundStage::Boundary | RoundStage::Publish => true,
                RoundStage::Wallets => {
                    round.wallets.is_empty()
                        || round
                            .wallets
                            .iter()
                            .any(|wallet| !busy_wallets.contains(wallet))
                }
                RoundStage::Done => false,
            };
            if !stopping && !backstop_busy && background_ready {
                continue;
            }
            let now = (self.now)();
            let retry_pending = !urgent_busy
                && refresh_reconcile
                    .keys()
                    .any(|wallet| !busy_wallets.contains(wallet));
            let retry_delay = Duration::from_nanos(1_000_000_000 - u64::from(now.nanosecond()));
            let retry_ready = tokio::time::Instant::now() + retry_delay;
            let wake = match (
                round.stage == RoundStage::Done && !backstop_busy,
                retry_pending,
            ) {
                (true, true) => Some(cadence.min(retry_ready)),
                (true, false) => Some(cadence),
                (false, true) => Some(retry_ready),
                (false, false) => None,
            };
            #[cfg(feature = "scenario")]
            if let Some(observer) = &self.wait_observer {
                let _ = observer.try_send(PollerWait {
                    obligations: self.obligations.clone(),
                    wake,
                    refresh_cooldown: self.refresh_cooldown.clone(),
                });
            }
            tokio::select! {
                biased;
                () = &mut shutdown, if !shutdown_requested => {
                    shutdown_requested = true;
                    stopping = true;
                    if let Some((_, cancel)) = &backstop_visit { let _ = cancel.send(true); }
                    if let Some(cancel) = &urgent_visit { let _ = cancel.send(true); }
                    if let Some((_, handoff, handle)) = &refresh_visit
                        && handoff.cancel_before_handoff() { handle.abort(); }
                }
                completed = tasks.join_next(), if !tasks.is_empty() => {
                    match completed {
                        Some(Ok(Completion::Reconciled { wallet, urgent, selected, acknowledged_audits, result })) => {
                            busy_wallets.remove(&wallet);
                            if urgent { urgent_busy = false; urgent_visit = None; }
                            else { backstop_busy = false; backstop_visit = None; }
                            let counts_round = if urgent {
                                if let Some(index) = round.wallets.iter().position(|candidate| *candidate == wallet) {
                                    round.wallets.remove(index);
                                    true
                                } else { false }
                            } else { true };
                            // A commitment precedes bucket application. Preserve its bindings even
                            // when application leaves targets outstanding for a later read.
                            for (epoch, groups) in &selected {
                                for obligation in groups.values() {
                                    if obligation.frame_admission_receipt.is_some() {
                                        insert_coalesced_obligation(&mut self.obligations.by_wallet, wallet, *epoch, obligation.clone());
                                    }
                                    if let Some(current) = self.obligations.by_wallet
                                        .get_mut(&wallet)
                                        .and_then(|epochs| epochs.get_mut(epoch))
                                        .and_then(|groups| groups.get_mut(&obligation.group_id.0))
                                        && current.receipt == obligation.receipt
                                    {
                                        current.bindings.clone_from(&obligation.bindings);
                                        current.retained_commitments.clone_from(&obligation.retained_commitments);
                                    }
                                }
                            }
                            if let Some(attempt) = attempts.get_mut(&wallet) {
                                attempt.selected.clone_from(&selected);
                            }
                            // A later fetch, cancellation, or bucket failure cannot undo an
                            // acknowledged incident or a retained disposed match.
                            for (epoch, obligation) in acknowledged_audits {
                                if !self.obligations.retired_frame_receipts.contains(&obligation.receipt) {
                                    self.obligations.retired_frame_receipts.push(obligation.receipt);
                                }
                                self.obligations.remove_selected(wallet, epoch, &obligation);
                                if let Some(attempt) = attempts.get_mut(&wallet) {
                                    remove_wallet_obligation(&mut attempt.selected, epoch, &obligation.group_id);
                                }
                            }
                            match result {
                                Err(ReconciliationError::Preempted) => {
                                    // The frozen frontier remains queued. The next urgent attempt
                                    // selects a new fixed end; the incomplete visit has no health vote.
                                    attempts.remove(&wallet);
                                }
                                Ok(resolved) => {
                                    if counts_round { round.successes += 1; }
                                    for (epoch, obligation) in resolved {
                                        if obligation.frame_admission_receipt.is_some() && !self.obligations.retired_frame_receipts.contains(&obligation.receipt) {
                                            self.obligations.retired_frame_receipts.push(obligation.receipt);
                                        }
                                        self.obligations.remove_selected(wallet, epoch, &obligation);
                                        if let Some(attempt) = attempts.get_mut(&wallet) {
                                            remove_wallet_obligation(&mut attempt.selected, epoch, &obligation.group_id);
                                        }
                                    }
                                    if attempts.get(&wallet).is_some_and(|attempt| attempt.selected.is_empty()) {
                                        attempts.remove(&wallet);
                                    }
                                    if refresh_reconcile.remove(&wallet).is_some() && !refresh_retry.contains(&wallet) {
                                        refresh_retry.push_back(wallet);
                                    }
                                }
                                Err(error) if error.retryable() => {
                                    if counts_round { round.failures += 1; }
                                    refresh_reconcile.remove(&wallet);
                                    if attempts.get(&wallet).is_some_and(|attempt| attempt.selected.is_empty()) {
                                        attempts.remove(&wallet);
                                    }
                                    warn!(wallet = %wallet, error = %error, "fixed-end activity reconciliation will retry");
                                }
                                Err(error) => failure = Some(TradePollerOwnerError::Reconciliation(error.to_string())),
                            }
                            #[cfg(feature = "scenario")]
                            self.report_progress(PollerProgress::Completed {
                                wallet,
                                selected: selected.values().flat_map(BTreeMap::values)
                                    .map(|obligation| obligation.receipt).collect(),
                                unresolved: self.obligations.unresolved_receipts(wallet),
                            });
                        }
                        Some(Ok(Completion::Refreshed(wallet, result, completed_at))) => {
                            busy_wallets.remove(&wallet);
                            refresh_busy = false;
                            refresh_visit = None;
                            match &result {
                                Ok((AnchorRefreshOutcome::Deferred, _)) => {
                                    self.refresh_cooldown.insert(wallet, completed_at + Duration::from_secs(ANCHOR_REFRESH_SECS));
                                    if refresh_pending == Some(wallet) { refresh_pending = None; }
                                }
                                Ok((AnchorRefreshOutcome::Anchored, _)) => {
                                    self.refresh_cooldown.remove(&wallet);
                                }
                                _ => {}
                            }
                            match result {
                                Ok((AnchorRefreshOutcome::Cancelled, _)) => {
                                    if !refresh_retry.contains(&wallet) { refresh_retry.push_back(wallet); }
                                }
                                Ok((AnchorRefreshOutcome::Deferred, true)) => { refresh_reconcile.entry(wallet).or_insert(None); }
                                Ok(_) => { refresh_reconcile.remove(&wallet); }
                                Err(error) => failure = Some(error),
                            }
                        }
                        Some(Ok(Completion::BoundaryInstalled(result))) => {
                            backstop_busy = false;
                            match result {
                                Ok(boundary) => { self.obligations.install_boundary(boundary); }
                                Err(error) => failure = Some(error),
                            }
                        }
                        Some(Ok(Completion::BoundaryPublished(boundary, result))) => {
                            backstop_busy = false;
                            match result {
                                Ok(()) => self.obligations.set_boundary_anchor(boundary.cutoff_unix),
                                Err(error) => { self.obligations.install_boundary(boundary); failure = Some(error); }
                            }
                        }
                        // Cancelled by shutdown: the operation's durable prefix stands on its own.
                        Some(Err(error)) if error.is_cancelled() => {
                            if let Some((wallet, handoff, _)) = refresh_visit.take()
                                && handoff.is_cancelled()
                            {
                                busy_wallets.remove(&wallet);
                                refresh_busy = false;
                                if !stopping && !refresh_retry.contains(&wallet) { refresh_retry.push_back(wallet); }
                            }
                        }
                        Some(Err(error)) => failure = Some(TradePollerOwnerError::Reconciliation(error.to_string())),
                        None => {}
                    }
                }
                trigger = self.trigger_rx.recv(), if !stopping => match trigger {
                    Some(trigger) => self.obligations.insert(trigger),
                    None => failure = Some(TradePollerOwnerError::TriggerChannelClosed),
                },
                () = async { if let Some(wake) = wake { tokio::time::sleep_until(wake).await; } else { std::future::pending::<()>().await; } }, if !stopping => {}
            }
            stopping |= failure.is_some();
        }
        failure.map_or(Ok(()), Err)
    }

    fn drain_triggers(&mut self) {
        for _ in 0..self.trigger_rx.max_capacity() {
            let Ok(trigger) = self.trigger_rx.try_recv() else {
                break;
            };
            self.obligations.insert(trigger);
        }
    }

    fn has_new_obligation(
        &self,
        wallet: WalletAddress,
        attempts: &HashMap<WalletAddress, WalletAttempt>,
    ) -> bool {
        let selected = attempts.get(&wallet).map(|attempt| &attempt.selected);
        self.obligations
            .by_wallet
            .get(&wallet)
            .is_some_and(|epochs| {
                epochs.iter().any(|(epoch, groups)| {
                    groups.iter().any(|(id, obligation)| {
                        selected
                            .and_then(|selected| selected.get(epoch))
                            .and_then(|selected| selected.get(id))
                            .is_none_or(|frozen| frozen.receipt != obligation.receipt)
                    })
                })
            })
    }

    fn freeze_attempt(&self, wallet: WalletAddress, urgent: bool) -> Option<WalletAttempt> {
        let now = (self.now)();
        let budget = time::Duration::seconds(
            i64::try_from(self.config.copy_latency_budget_secs).unwrap_or(i64::MAX),
        );
        let selected = self
            .obligations
            .by_wallet
            .get(&wallet)
            .cloned()
            .unwrap_or_default();
        if urgent
            && (selected.is_empty()
                || (self.config.activity_ws_enabled
                    && !selected.keys().any(|epoch| {
                        OffsetDateTime::from_unix_timestamp(*epoch)
                            .is_ok_and(|source| now - source <= budget)
                    })))
        {
            return None;
        }
        let deadline = self
            .config
            .activity_ws_enabled
            .then(|| {
                selected
                    .first_key_value()
                    .and_then(|(epoch, _)| OffsetDateTime::from_unix_timestamp(*epoch).ok())
                    .and_then(|source| source.checked_add(budget))
            })
            .flatten();
        Some(WalletAttempt {
            selected,
            deadline,
            last_fixed_end: None,
        })
    }

    fn operation(&self) -> WalletOperation {
        WalletOperation {
            config: self.config.clone(),
            fetcher: self.fetcher.clone(),
            asset_identity: self.asset_identity.clone(),
            source_log: self.source_log.clone(),
            source_receipts: self.source_receipts.clone(),
            #[cfg(feature = "scenario")]
            crash_boundary: self.crash_boundary.clone(),
            control_tx: self.control_tx.clone(),
            paper_state: self.paper_state.clone(),
            signal_config: self.signal_config.clone(),
            runtime_config: self.runtime_config.clone(),
            now: self.now.clone(),
            admission_preparer: self.admission_preparer.clone(),
        }
    }

    fn spawn_reconciliation(
        &self,
        tasks: &mut JoinSet<Completion>,
        wallet: WalletAddress,
        attempt: &WalletAttempt,
        urgent: bool,
        fixed_end: i64,
        mut cancel: Option<watch::Receiver<bool>>,
    ) {
        let operation = self.operation();
        let mut selected = attempt.selected.clone();
        #[cfg(feature = "scenario")]
        let frontier = selected
            .values()
            .flat_map(BTreeMap::values)
            .map(|obligation| obligation.receipt)
            .collect::<Vec<_>>();
        let entry = self
            .live_watchlist
            .snapshot()
            .entries
            .iter()
            .find(|entry| entry.wallet == wallet)
            .cloned();
        #[cfg(feature = "scenario")]
        self.report_progress(PollerProgress::Started {
            wallet,
            urgent,
            frontier: frontier.clone(),
        });
        tasks.spawn(async move {
            let mut acknowledged_audits = Vec::new();
            let result = operation
                .reconcile_wallet(
                    wallet,
                    entry.as_ref(),
                    &mut selected,
                    fixed_end,
                    &mut cancel,
                    &mut acknowledged_audits,
                )
                .await;
            Completion::Reconciled {
                wallet,
                urgent,
                selected,
                acknowledged_audits,
                result,
            }
        });
    }

    fn record_round_health(&self, round: &BackstopRound) {
        if round.successes == 0 && round.failures == 0 {
            return;
        }
        let mut health = self
            .health
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if round.successes > 0 {
            let now = (self.now)();
            health.polymarket_last_event_at = Some(now);
            health.poll_last_round_at = Some(now);
            health.poll_error_streak = 0;
        } else {
            health.poll_error_streak = health.poll_error_streak.saturating_add(1);
        }
    }

    #[cfg(feature = "scenario")]
    fn report_progress(&self, progress: PollerProgress) {
        if let Some(sender) = &self.progress {
            let _ = sender.try_send(progress);
        }
    }

    fn select_refresh_wallet(
        &mut self,
        wallets: &[WalletAddress],
    ) -> Result<Option<WalletAddress>, TradePollerOwnerError> {
        if self.admission_preparer.is_none() {
            return Ok(None);
        }
        if wallets.is_empty() {
            self.refresh_cursor = 0;
            return Ok(None);
        }
        let now_unix = (self.now)().unix_timestamp();
        let mut candidates = Vec::new();
        for offset in 0..wallets.len() {
            let index = self.refresh_cursor.saturating_add(offset) % wallets.len();
            let wallet = wallets[index];
            if self
                .refresh_cooldown
                .get(&wallet)
                .is_some_and(|deadline| tokio::time::Instant::now() < *deadline)
            {
                continue;
            }
            let coverage = self
                .paper_state
                .wallet_coverage(&wallet)
                .map_err(|error| TradePollerOwnerError::AnchorRefresh(error.to_string()))?;
            if !anchor_refresh_due(&coverage, now_unix, ANCHOR_REFRESH_SECS) {
                continue;
            }
            let class = if coverage.reanchor_required {
                AnchorRefreshClass::ReanchorRequired
            } else {
                AnchorRefreshClass::AgeDue
            };
            candidates.push((index, class));
        }
        let Some(index) = select_refresh_candidate(&candidates, &mut self.refresh_reanchor_turn)
        else {
            return Ok(None);
        };
        let wallet = wallets[index];
        self.refresh_cursor = index.saturating_add(1) % wallets.len();
        Ok(Some(wallet))
    }
}

#[derive(Clone)]
struct WalletOperation {
    config: TradePollerConfig,
    fetcher: Arc<dyn ReconciliationFetcher>,
    asset_identity: Arc<AssetIdentityResolver>,
    source_log: SourceLogHandle,
    source_receipts: Option<SourceReceiptIndex>,
    #[cfg(feature = "scenario")]
    crash_boundary: Option<Arc<Mutex<Option<ReconciliationCrashBoundary>>>>,
    control_tx: mpsc::Sender<OrchestratorControl>,
    paper_state: Arc<PaperStateDb>,
    signal_config: SignalConfig,
    runtime_config: LiveRuntimeConfig,
    now: Arc<dyn Fn() -> OffsetDateTime + Send + Sync>,
    admission_preparer: Option<AdmissionPreparer>,
}

impl WalletOperation {
    #[cfg(feature = "scenario")]
    fn crash_after(
        &self,
        boundary: ReconciliationCrashBoundary,
    ) -> Result<(), ReconciliationError> {
        if self.crash_boundary.as_ref().is_some_and(|selected| {
            let mut selected = selected
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if *selected == Some(boundary) {
                *selected = None;
                true
            } else {
                false
            }
        }) {
            Err(ReconciliationError::InjectedCrash(boundary))
        } else {
            Ok(())
        }
    }

    async fn cancellable<T>(
        future: impl Future<Output = T>,
        cancel: &mut Option<watch::Receiver<bool>>,
    ) -> Result<T, ReconciliationError> {
        if let Some(cancel) = cancel {
            tokio::select! {
                biased;
                () = wait_for_cancel(cancel) => Err(ReconciliationError::Preempted),
                output = future => Ok(output),
            }
        } else {
            Ok(future.await)
        }
    }

    async fn refresh_one_wallet(
        &self,
        wallet: WalletAddress,
        handoff: &RefreshHandoff,
    ) -> Result<(AnchorRefreshOutcome, bool), TradePollerOwnerError> {
        let Some(preparer) = &self.admission_preparer else {
            return Ok((AnchorRefreshOutcome::Skipped, false));
        };
        let coverage = self
            .paper_state
            .wallet_coverage(&wallet)
            .map_err(|error| TradePollerOwnerError::AnchorRefresh(error.to_string()))?;
        let routine = coverage.anchor_seq.is_some() && !coverage.reanchor_required;
        let outcome = preparer
            .prepare_if_due_observed(
                wallet,
                (self.now)().unix_timestamp(),
                ANCHOR_REFRESH_SECS,
                Some(handoff),
            )
            .await
            .map_err(|error| TradePollerOwnerError::AnchorRefresh(error.to_string()))?;
        Ok((outcome, routine))
    }

    async fn install_boundary(
        &self,
        cutoff_unix: i64,
    ) -> Result<PendingBoundary, TradePollerOwnerError> {
        let now = (self.now)();
        let receipt = self
            .source_log
            .append(EnvelopeIn {
                source_id: SourceId(DAILY_BOUNDARY_SOURCE_ID.to_owned()),
                schema_version: 1,
                parser_version: 1,
                observed_at: SourceTimestamp(now),
                received_at: ReceivedAt(now),
                content_type: ContentType::Json,
                payload: serde_json::to_vec(
                    &serde_json::json!({ "kind": "daily_boundary", "cutoff_unix": cutoff_unix }),
                )
                .map_err(|error| TradePollerOwnerError::DailyBoundary(error.to_string()))?,
            })
            .await
            .map_err(|error| TradePollerOwnerError::DailyBoundary(error.to_string()))?;
        Ok(PendingBoundary {
            cutoff_unix,
            receipt,
        })
    }

    async fn publish_boundary(
        &self,
        boundary: PendingBoundary,
    ) -> Result<(), TradePollerOwnerError> {
        let (acknowledged, received) = oneshot::channel();
        self.control_tx
            .send(OrchestratorControl::DailyBoundary {
                cutoff_unix: boundary.cutoff_unix,
                boundary_receipt: boundary.receipt,
                acknowledged,
            })
            .await
            .map_err(|_| {
                TradePollerOwnerError::DailyBoundary(
                    "orchestrator control channel closed".to_owned(),
                )
            })?;
        received
            .await
            .map_err(|_| {
                TradePollerOwnerError::DailyBoundary(
                    "daily boundary acknowledgement closed".to_owned(),
                )
            })?
            .map_err(TradePollerOwnerError::DailyBoundary)
    }

    async fn reconcile_wallet(
        &self,
        wallet: WalletAddress,
        entry: Option<&pe_trader_index::WatchlistEntry>,
        selected: &mut WalletObligations,
        fixed_end: i64,
        cancel: &mut Option<watch::Receiver<bool>>,
        acknowledged_audits: &mut Vec<(i64, Obligation)>,
    ) -> Result<Vec<(i64, Obligation)>, ReconciliationError> {
        let initial_frames = admitted_frame_obligations(&self.paper_state, Some(wallet))?;
        for (frame_wallet, epoch, obligation) in &initial_frames {
            if *frame_wallet == wallet {
                replace_admitted_obligation(selected, *epoch, obligation.clone(), false);
            }
        }
        let mut negative_resolved = Vec::new();
        let mut retained_matched = Vec::new();
        if let Some(index) = &self.source_receipts {
            let frames = self
                .paper_state
                .activity_frame_decision_index(Some(&wallet))?
                .into_iter()
                .map(|frame| (frame.source_trade_id.clone(), frame))
                .collect::<HashMap<_, _>>();
            let obligations = selected
                .iter()
                .flat_map(|(epoch, groups)| {
                    groups
                        .values()
                        .filter(|obligation| obligation.frame_admission_receipt.is_some())
                        .map(move |obligation| (*epoch, obligation.clone()))
                })
                .collect::<Vec<_>>();
            let mut receipts = obligations
                .iter()
                .flat_map(|(_, obligation)| obligation.retained_commitments.iter().copied())
                .collect::<Vec<_>>();
            receipts.sort_by_key(|receipt| receipt.sequence);
            receipts.dedup();
            let mut matched = HashSet::new();
            let mut negatives = Vec::new();
            // Authenticate each retained read once, then release its reconstructed history.
            // Matches are applied before any retained or later absence is considered.
            for receipt in receipts {
                let read = Arc::new(
                    crate::bucket_commit::verified_commitment_bindings(receipt, index)
                        .map_err(|error| ReconciliationError::Binding(error.to_string()))?,
                );
                let mut matches = Vec::new();
                for (epoch, obligation) in &obligations {
                    if matched.contains(&obligation.receipt.sequence) {
                        continue;
                    }
                    let frame = frames.get(&obligation.group_id).ok_or_else(|| {
                        ReconciliationError::Binding("admitted frame disappeared".to_owned())
                    })?;
                    let conclusion = crate::feed_audit::disposition(frame, &read)
                        .map_err(|error| ReconciliationError::Binding(error.to_string()))?;
                    match conclusion {
                        crate::feed_audit::AuditDisposition::Matched(id) => {
                            matches.push((*epoch, obligation.clone(), id))
                        }
                        crate::feed_audit::AuditDisposition::Absent
                        | crate::feed_audit::AuditDisposition::Contradicted(_) => {
                            negatives.push((*epoch, obligation.clone(), receipt, conclusion));
                        }
                        crate::feed_audit::AuditDisposition::Unresolved => {}
                    }
                }
                let mut needs_commit = false;
                for (_, obligation) in &obligations {
                    for binding in read
                        .bindings
                        .iter()
                        .filter(|binding| binding.stream_receipt == obligation.receipt)
                    {
                        needs_commit |= !binding_target_disposed(&self.paper_state, binding)?;
                    }
                }
                if needs_commit {
                    self.commit_retained_read(read.clone(), selected, entry, cancel)
                        .await?;
                }
                // Retained routing also dispositions ordinary observations, including a
                // first-seen ambiguity fence. A later failed fetch cannot leave their barriers.
                self.promote_selected_admissions(wallet, selected)?;
                let ordinary = self.disposed_obligations(wallet, selected, &read)?;
                for (epoch, obligation) in ordinary
                    .into_iter()
                    .filter(|(_, obligation)| obligation.frame_admission_receipt.is_none())
                {
                    if self
                        .acknowledge_retirement(&obligation, Some(read.clone()))
                        .await?
                    {
                        acknowledged_audits.push((epoch, obligation.clone()));
                        remove_wallet_obligation(selected, epoch, &obligation.group_id);
                    }
                }
                self.promote_selected_admissions(wallet, selected)?;
                for (epoch, obligation, id) in matches {
                    let binding = read
                        .binding_indices
                        .get(&(obligation.receipt.sequence, obligation.receipt.this_hash))
                        .and_then(|index| read.bindings.get(*index))
                        .filter(|binding| binding.history_group_id == id)
                        .ok_or_else(|| {
                            ReconciliationError::Binding(
                                "retained match binding missing".to_owned(),
                            )
                        })?;
                    if !binding_target_disposed(&self.paper_state, binding)? {
                        continue; // ordinary routing stopped at an unresolved observation or fence
                    }
                    if !self
                        .acknowledge_retirement(&obligation, Some(read.clone()))
                        .await?
                    {
                        self.promote_selected_admissions(wallet, selected)?;
                        continue;
                    }
                    matched.insert(obligation.receipt.sequence);
                    acknowledged_audits.push((epoch, obligation.clone()));
                    retained_matched.push((epoch, obligation));
                }
            }
            let mut negative_receipts = HashSet::new();
            negatives.retain(|(_, obligation, _, _)| {
                !matched.contains(&obligation.receipt.sequence)
                    && negative_receipts.insert(obligation.receipt.sequence)
            });
            // A negative keeps only its deciding receipt while match search is in progress.
            // Group by read to avoid reconstructing a deciding proof per obligation.
            negatives.sort_by_key(|(_, _, receipt, _)| receipt.sequence);
            let mut pending = negatives.into_iter().peekable();
            while let Some((_, _, receipt, _)) = pending.peek() {
                let receipt = *receipt;
                let read = Arc::new(
                    crate::bucket_commit::verified_commitment_bindings(receipt, index)
                        .map_err(|error| ReconciliationError::Binding(error.to_string()))?,
                );
                while pending
                    .peek()
                    .is_some_and(|(_, _, deciding, _)| *deciding == receipt)
                {
                    let Some((epoch, obligation, _, conclusion)) = pending.next() else {
                        break;
                    };
                    if self
                        .request_negative_audit(
                            obligation.receipt,
                            receipt,
                            &conclusion,
                            Some(read.clone()),
                        )
                        .await?
                        && self.negative_target_disposed(&obligation, &conclusion, &read)?
                    {
                        acknowledged_audits.push((epoch, obligation.clone()));
                        negative_resolved.push((epoch, obligation));
                    }
                }
            }
        }
        let mut has_frame_audit = selected
            .values()
            .flat_map(BTreeMap::values)
            .any(|obligation| {
                obligation.frame_admission_receipt.is_some()
                    && !negative_resolved
                        .iter()
                        .chain(&retained_matched)
                        .any(|(_, resolved)| resolved.receipt == obligation.receipt)
            });
        let cursor_start = self
            .paper_state
            .cursor(&wallet)?
            .map(|value| value.saturating_sub(1));
        let obligation_start = selected
            .first_key_value()
            .map(|(epoch, _)| epoch.saturating_sub(1));
        let mut start = if has_frame_audit {
            Some(0)
        } else {
            match (cursor_start, obligation_start) {
                (Some(left), Some(right)) => Some(left.min(right)),
                (left, None) => left,
                (None, Some(_)) => None,
            }
        };
        // A delivery cursor can be ahead of the contiguous feed frontier after an audit.
        // Keep the next ordinary read contiguous instead of turning successful work into
        // a publication failure, while unresolved frame absence still uses (0, fixed_end].
        if !has_frame_audit && let Some(index) = &self.source_receipts {
            let collection: crate::frame_admission::FrontierCollection =
                serde_json::from_value(self.paper_state.feed_history_frontiers()?)?;
            if collection.version != 1 {
                return Err(ReconciliationError::Binding(
                    "unsupported frontier collection".to_owned(),
                ));
            }
            if let Some(frontier) = collection
                .frontiers
                .iter()
                .find(|frontier| frontier.wallet == wallet)
            {
                index
                    .verify_frame_frontier(frontier)
                    .map_err(|error| ReconciliationError::Binding(error.to_string()))?;
                start = start.map(|start| start.min(frontier.fixed_end.saturating_sub(1)));
            }
        }
        let append_closed = Arc::new(AtomicBool::new(false));
        let recording = RecordingFetcher {
            inner: self.fetcher.clone(),
            source_log: self.source_log.clone(),
            append_closed: append_closed.clone(),
            occurrences: Arc::new(Mutex::new(Vec::new())),
            now: self.now.clone(),
        };
        let activity = match Self::cancellable(
            fetch_complete_activity(&recording, &self.config.base_url, wallet, start, fixed_end),
            cancel,
        )
        .await?
        {
            Ok(activity) => activity,
            Err(_) if append_closed.load(Ordering::Acquire) => {
                return Err(ReconciliationError::SourceLogClosed);
            }
            Err(error) => return Err(error.into()),
        };
        let page_occurrences = recording.join_occurrences(&activity.pages)?;
        let buckets = activity.buckets()?;
        let restamp_twins = restamp_twins(&self.paper_state, &activity.rows)?;
        // Resolve every required token and record its metadata before freezing the read commitment.
        let mut identities = Vec::with_capacity(buckets.len());
        for bucket in &buckets {
            identities.push(Self::cancellable(self.resolve_bucket(bucket), cancel).await??);
        }
        // Admission can commit during either network acquisition. Refresh the durable
        // authority before correlating this read, including triggers selected before commit.
        let frames = admitted_frame_obligations(&self.paper_state, Some(wallet))?;
        for (frame_wallet, epoch, obligation) in &frames {
            if *frame_wallet == wallet {
                let newly_admitted = !initial_frames
                    .iter()
                    .any(|(_, _, initial)| initial.receipt == obligation.receipt);
                replace_admitted_obligation(selected, *epoch, obligation.clone(), newly_admitted);
            }
        }
        has_frame_audit |= selected
            .values()
            .flat_map(BTreeMap::values)
            .any(|obligation| {
                obligation.frame_admission_receipt.is_some()
                    && !negative_resolved
                        .iter()
                        .chain(&retained_matched)
                        .any(|(_, resolved)| resolved.receipt == obligation.receipt)
            });
        let mut correlation_candidates = selected.clone();
        for (frame_wallet, epoch, obligation) in frames {
            if frame_wallet == wallet {
                replace_admitted_obligation(&mut correlation_candidates, epoch, obligation, true);
            }
        }
        let mut correlation = self.correlate(
            wallet,
            &correlation_candidates,
            &activity.rows,
            &restamp_twins,
            &buckets.iter().flatten().collect::<Vec<_>>(),
            &identities,
            &page_occurrences,
            &activity.pages,
        )?;
        let bindings = correlation
            .matched
            .iter()
            .map(|matched| matched.binding.clone())
            .collect::<Vec<_>>();
        // Bindings retain their proof through the shared encoder. An empty binding set
        // needs a proof only when this complete full-history read can decide absence;
        // immature or ambiguous reads remain ordinary proofless frontier commitments.
        let mut retain_proof = false;
        for obligation in selected
            .values()
            .flat_map(BTreeMap::values)
            .filter(|obligation| obligation.frame_admission_receipt.is_some())
        {
            if negative_resolved
                .iter()
                .chain(&retained_matched)
                .any(|(_, resolved)| resolved.receipt == obligation.receipt)
            {
                continue;
            }
            let frame = self
                .paper_state
                .activity_frame_decision(&obligation.group_id)?
                .ok_or_else(|| {
                    ReconciliationError::Binding("admitted frame disappeared".to_owned())
                })?;
            let maturity = i64::try_from(frame.copy_latency_budget_secs)
                .ok()
                .and_then(|budget| frame.source_epoch.checked_add(budget))
                .ok_or_else(|| {
                    ReconciliationError::Binding("audit maturity overflow".to_owned())
                })?;
            retain_proof |= start == Some(0)
                && fixed_end >= maturity
                && !buckets.iter().flatten().any(|aggregate| {
                    let components = aggregate.group_id.components();
                    components.wallet == wallet
                        && components.transaction_hash == frame.transaction_hash
                        && components.activity_type == ActivityType::Trade
                });
        }
        // Once queued, the coordinator owns the append. Drain and authenticate its
        // acknowledgement before yielding, just as for a sent bucket commit.
        let read_commitment = self
            .append_read_commitment(
                wallet,
                fixed_end,
                &page_occurrences,
                &activity.pages,
                &bindings,
                retain_proof,
            )
            .await?;
        #[cfg(feature = "scenario")]
        self.crash_after(ReconciliationCrashBoundary::Commitment)?;
        let index = self.source_receipts.as_ref().ok_or_else(|| {
            ReconciliationError::Binding("source receipt index missing".to_owned())
        })?;
        let verified = std::sync::Arc::new(
            crate::bucket_commit::verified_read_for_routing(
                read_commitment,
                wallet,
                fixed_end,
                &page_occurrences,
                &activity.pages,
                index,
            )
            .map_err(|error| ReconciliationError::Binding(error.to_string()))?,
        );
        for matched in &correlation.matched {
            if let Some(obligation) = selected
                .get_mut(&matched.epoch)
                .and_then(|groups| groups.get_mut(&matched.binding.stream_group_id.0))
                && !obligation.bindings.contains(&matched.binding)
            {
                obligation.bindings.push(matched.binding.clone());
                obligation.retained_commitments.push(read_commitment);
            }
        }
        if has_frame_audit {
            if retain_proof || !bindings.is_empty() {
                for obligation in selected
                    .values_mut()
                    .flat_map(BTreeMap::values_mut)
                    .filter(|obligation| obligation.frame_admission_receipt.is_some())
                {
                    obligation.retained_commitments.push(read_commitment);
                }
            }
            if cancel.as_ref().is_some_and(|cancel| *cancel.borrow()) {
                return Err(ReconciliationError::Preempted);
            }
            for (epoch, groups) in selected.iter() {
                for obligation in groups
                    .values()
                    .filter(|obligation| obligation.frame_admission_receipt.is_some())
                {
                    if negative_resolved
                        .iter()
                        .chain(&retained_matched)
                        .any(|(_, resolved)| resolved.receipt == obligation.receipt)
                    {
                        continue;
                    }
                    let frame = self
                        .paper_state
                        .activity_frame_decision(&obligation.group_id)?
                        .ok_or_else(|| {
                            ReconciliationError::Binding("admitted frame disappeared".to_owned())
                        })?;
                    let conclusion = crate::feed_audit::disposition(&frame, &verified)
                        .map_err(|error| ReconciliationError::Binding(error.to_string()))?;
                    if !self
                        .request_negative_audit(
                            obligation.receipt,
                            read_commitment,
                            &conclusion,
                            Some(verified.clone()),
                        )
                        .await?
                    {
                        continue;
                    }
                    if self.negative_target_disposed(obligation, &conclusion, &verified)? {
                        acknowledged_audits.push((*epoch, obligation.clone()));
                        negative_resolved.push((*epoch, obligation.clone()));
                    }
                }
            }
        }
        correlation.unmatched.retain(|(_, id)| {
            selected.values().any(|groups| groups.contains_key(&id.0))
                && !negative_resolved
                    .iter()
                    .chain(&retained_matched)
                    .any(|(_, obligation)| &obligation.group_id == id)
        });
        correlation.unmatched_epoch = correlation.unmatched.iter().map(|(epoch, _)| *epoch).min();
        if let Some(latest) = buckets
            .iter()
            .flatten()
            .map(|aggregate| aggregate.source_time.0.unix_timestamp())
            .max()
        {
            self.paper_state.set_activity(&wallet, latest)?;
        }
        let quality = match entry {
            Some(entry) => entry.reconstruction_quality,
            None => ReconstructionQuality::new(0)
                .map_err(|_| ReconciliationError::ReconstructionQuality)?,
        };
        let copy_eligible = entry.is_some_and(|entry| entry.tier == WatchlistTier::Active);
        let all_acknowledged = self
            .route_authenticated_read(
                buckets.into_iter(),
                identities,
                &restamp_twins,
                &correlation,
                verified.clone(),
                quality,
                copy_eligible,
                cancel,
            )
            .await?;
        if cancel.as_ref().is_some_and(|cancel| *cancel.borrow()) {
            return Err(ReconciliationError::Preempted);
        }
        // Admission can synchronize during commitment append or any bucket await. Promote
        // before considering disposal, then let the serialized owner reject a stale receipt.
        self.promote_selected_admissions(wallet, selected)?;
        if has_frame_audit {
            for (epoch, groups) in selected.iter() {
                for obligation in groups.values() {
                    if obligation.frame_admission_receipt.is_none()
                        || negative_resolved
                            .iter()
                            .any(|(_, resolved)| resolved.receipt == obligation.receipt)
                    {
                        continue;
                    }
                    let frame = self
                        .paper_state
                        .activity_frame_decision(&obligation.group_id)?
                        .ok_or_else(|| {
                            ReconciliationError::Binding("admitted frame disappeared".to_owned())
                        })?;
                    let conclusion = crate::feed_audit::disposition(&frame, &verified)
                        .map_err(|error| ReconciliationError::Binding(error.to_string()))?;
                    if self.negative_target_disposed(obligation, &conclusion, &verified)?
                        && self
                            .request_negative_audit(
                                obligation.receipt,
                                verified.receipt,
                                &conclusion,
                                Some(verified.clone()),
                            )
                            .await?
                    {
                        acknowledged_audits.push((*epoch, obligation.clone()));
                        negative_resolved.push((*epoch, obligation.clone()));
                    }
                }
            }
        }
        let disposed = self.disposed_obligations(wallet, selected, &verified)?;
        let mut resolved = Vec::new();
        for (epoch, obligation) in disposed {
            if self
                .acknowledge_retirement(&obligation, Some(verified.clone()))
                .await?
            {
                acknowledged_audits.push((epoch, obligation.clone()));
                resolved.push((epoch, obligation));
            }
        }
        self.promote_selected_admissions(wallet, selected)?;
        // Publication reads the owner's barrier after all acknowledged retirements.
        if all_acknowledged
            && correlation.unmatched_epoch.is_none()
            && correlation.ambiguous.is_empty()
            && selected
                .values()
                .flat_map(BTreeMap::values)
                .all(|obligation| {
                    resolved
                        .iter()
                        .chain(&negative_resolved)
                        .chain(&retained_matched)
                        .any(|(_, retired)| retired.receipt == obligation.receipt)
                })
        {
            self.publish_feed_frontier(
                wallet,
                fixed_end,
                read_commitment,
                &page_occurrences,
                &activity.pages,
                Some(verified.clone()),
            )
            .await?;
        }
        resolved.extend(negative_resolved);
        resolved.extend(retained_matched);
        Ok(resolved)
    }

    #[allow(clippy::too_many_arguments)]
    async fn route_authenticated_read(
        &self,
        buckets: impl Iterator<Item = Vec<ActivityAggregate>> + Send,
        identities: Vec<BucketIdentities>,
        restamp_twins: &HashSet<SourceTradeId>,
        correlation: &Correlation,
        read: Arc<crate::bucket_commit::VerifiedCommitment>,
        quality: ReconstructionQuality,
        copy_eligible: bool,
        cancel: &mut Option<watch::Receiver<bool>>,
    ) -> Result<bool, ReconciliationError> {
        let frontier = read
            .frontier
            .as_ref()
            .ok_or_else(|| ReconciliationError::Binding("routing read lacks proof".to_owned()))?;
        // Only unmatched observations block ordering. Matched observations use the endpoint's
        // bucket clock; the original stream second remains in the binding and the age check.
        let mut all_acknowledged = true;
        for (bucket, identities) in buckets.zip(identities) {
            if cancel.as_ref().is_some_and(|cancel| *cancel.borrow()) {
                return Err(ReconciliationError::Preempted);
            }
            let source_epoch = bucket_epoch(&bucket)?;
            if correlation
                .unmatched_epoch
                .is_some_and(|epoch| source_epoch >= epoch)
                && correlation.ambiguous.is_empty()
            {
                all_acknowledged = false;
                break;
            }
            let mut context = self.context(
                read.fixed_end,
                quality,
                copy_eligible,
                &frontier.pages,
                &frontier.page_occurrences,
                read.receipt,
                &bucket,
                identities,
                &correlation.matched,
            )?;
            context.verified_read = Some(read.clone());
            context.restamp_twins = bucket
                .iter()
                .map(|aggregate| aggregate.group_id.key())
                .filter(|group| restamp_twins.contains(*group))
                .cloned()
                .collect();
            if !correlation.ambiguous.is_empty() {
                let mut inputs: serde_json::Value =
                    serde_json::from_str(&context.decision_inputs_json)?;
                inputs["invalid_mapping_observations"] =
                    serde_json::to_value(&correlation.ambiguous)?;
                context.decision_inputs_json = serde_json::to_string(&inputs)?;
            }
            let result = self.commit_bucket(bucket, context).await?;
            #[cfg(feature = "scenario")]
            self.crash_after(ReconciliationCrashBoundary::Bucket)?;
            // A sent commit is never abandoned: its acknowledgement or failure is established
            // before the coordinator can hand the wallet to urgent reconciliation.
            if cancel.as_ref().is_some_and(|cancel| *cancel.borrow()) {
                return Err(ReconciliationError::Preempted);
            }
            if result.newly_fenced.is_some() {
                all_acknowledged = false;
                break;
            }
        }
        Ok(all_acknowledged)
    }

    async fn commit_retained_read(
        &self,
        read: Arc<crate::bucket_commit::VerifiedCommitment>,
        selected: &WalletObligations,
        entry: Option<&pe_trader_index::WatchlistEntry>,
        cancel: &mut Option<watch::Receiver<bool>>,
    ) -> Result<(), ReconciliationError> {
        let frontier = read.frontier.as_ref().ok_or_else(|| {
            ReconciliationError::Binding("retained match lacks read proof".to_owned())
        })?;
        let index = self.source_receipts.as_ref().ok_or_else(|| {
            ReconciliationError::Binding("source receipt index missing".to_owned())
        })?;
        let quality = match entry {
            Some(entry) => entry.reconstruction_quality,
            None => ReconstructionQuality::new(0)
                .map_err(|_| ReconciliationError::ReconstructionQuality)?,
        };
        let bound = read
            .bindings
            .iter()
            .map(|binding| (&binding.history_group_id, binding))
            .collect::<HashMap<_, _>>();
        let rows = crate::bucket_commit::retained_read_rows(&read, index)
            .map_err(|error| ReconciliationError::Binding(error.to_string()))?;
        let restamp_twins = restamp_twins(&self.paper_state, &rows)?;
        let mut bucket_identities = Vec::new();
        // Sort references, retaining only the current bucket's owned routing input.
        let mut ordered = read.aggregates.iter().collect::<Vec<_>>();
        ordered.sort_by_key(|aggregate| aggregate.source_time.0);
        for group in ordered.chunk_by(|left, right| left.source_time == right.source_time) {
            let bucket = group
                .iter()
                .map(|aggregate| (*aggregate).clone())
                .collect::<Vec<_>>();
            let unbound = bucket
                .iter()
                .filter(|aggregate| !bound.contains_key(aggregate.group_id.key()))
                .cloned()
                .collect::<Vec<_>>();
            let mut identities = if unbound.is_empty() {
                BucketIdentities::default()
            } else {
                Self::cancellable(self.resolve_bucket(&unbound), cancel).await??
            };
            // Reuse authenticated effective identities for bound targets. A later metadata
            // response cannot change the recorded match between sync and bucket commit.
            for aggregate in &bucket {
                let id = aggregate.group_id.key();
                if let Some(verified) = read.identities.get(id) {
                    let components = aggregate.group_id.components();
                    let raw = components
                        .condition_id
                        .as_ref()
                        .zip(components.outcome)
                        .map(|(condition, outcome)| {
                            MarketOutcomeId::new(
                                MarketId(VenueMarketId(condition.0.clone())),
                                outcome,
                            )
                        });
                    if raw.as_ref() != Some(verified) {
                        let evidence_hash = bound
                            .get(id)
                            .and_then(|binding| binding.identity_provenance.as_ref())
                            .map(|provenance| provenance.canonical_page_hash.clone())
                            .ok_or_else(|| {
                                ReconciliationError::Binding(
                                    "retained effective identity lacks authenticated provenance"
                                        .to_owned(),
                                )
                            })?;
                        identities.overrides.insert(
                            id.clone(),
                            IdentityOverride {
                                verified: verified.clone(),
                                evidence_hash,
                            },
                        );
                    }
                }
            }
            bucket_identities.push(identities);
        }
        let mut candidates = selected.clone();
        for (_, epoch, obligation) in
            admitted_frame_obligations(&self.paper_state, Some(read.wallet))?
        {
            replace_admitted_obligation(&mut candidates, epoch, obligation, true);
        }
        let mut correlation = self.correlate(
            read.wallet,
            &candidates,
            &rows,
            &restamp_twins,
            &ordered,
            &bucket_identities,
            &frontier.page_occurrences,
            &frontier.pages,
        )?;
        // The durable commitment is the authority for already bound observations. Current
        // discovery supplies barriers/fences but never rewrites their original proof.
        for matched in &mut correlation.matched {
            if let Some(binding) = read
                .binding_indices
                .get(&(
                    matched.binding.stream_receipt.sequence,
                    matched.binding.stream_receipt.this_hash,
                ))
                .and_then(|position| read.bindings.get(*position))
            {
                matched.binding.clone_from(binding);
            }
        }
        self.route_authenticated_read(
            ordered
                .chunk_by(|left, right| left.source_time == right.source_time)
                .map(|group| group.iter().map(|aggregate| (*aggregate).clone()).collect()),
            bucket_identities,
            &restamp_twins,
            &correlation,
            read.clone(),
            quality,
            entry.is_some_and(|entry| entry.tier == WatchlistTier::Active),
            cancel,
        )
        .await?;
        Ok(())
    }

    async fn acknowledge_retirement(
        &self,
        obligation: &Obligation,
        verified_read: Option<std::sync::Arc<crate::bucket_commit::VerifiedCommitment>>,
    ) -> Result<bool, ReconciliationError> {
        let (acknowledged, response) = oneshot::channel();
        self.control_tx
            .send(OrchestratorControl::FeedAuditUpdate {
                update: crate::orchestrator_control::FeedAuditUpdate::RetireObservation {
                    receipt: obligation.receipt,
                    source_trade_id: obligation.group_id.clone(),
                    unbound: obligation.bindings.is_empty(),
                    verified_read,
                },
                acknowledged,
            })
            .await
            .map_err(|_| ReconciliationError::ControlClosed)?;
        response
            .await
            .map_err(|_| ReconciliationError::ControlClosed)?
            .map(|ack| {
                matches!(
                    ack,
                    crate::orchestrator_control::FeedAuditAcknowledgement::Applied
                )
            })
            .map_err(ReconciliationError::BucketCommit)
    }

    fn negative_target_disposed(
        &self,
        obligation: &Obligation,
        conclusion: &crate::feed_audit::AuditDisposition,
        read: &crate::bucket_commit::VerifiedCommitment,
    ) -> Result<bool, ReconciliationError> {
        match conclusion {
            crate::feed_audit::AuditDisposition::Absent => Ok(true),
            crate::feed_audit::AuditDisposition::Contradicted(id) => {
                let binding = read
                    .binding_indices
                    .get(&(obligation.receipt.sequence, obligation.receipt.this_hash))
                    .and_then(|position| read.bindings.get(*position))
                    .filter(|binding| &binding.history_group_id == id)
                    .ok_or_else(|| {
                        ReconciliationError::Binding(
                            "contradicted audit binding missing".to_owned(),
                        )
                    })?;
                binding_target_disposed(&self.paper_state, binding).map_err(Into::into)
            }
            _ => Ok(false),
        }
    }

    async fn request_negative_audit(
        &self,
        frame_receipt: AppendReceipt,
        deciding_commitment_receipt: AppendReceipt,
        conclusion: &crate::feed_audit::AuditDisposition,
        verified_read: Option<std::sync::Arc<crate::bucket_commit::VerifiedCommitment>>,
    ) -> Result<bool, ReconciliationError> {
        let (cause, counterpart_identity) = match conclusion {
            crate::feed_audit::AuditDisposition::Contradicted(id) => (
                crate::paper_recovery::FeedIncidentCause::Contradiction,
                Some(id.clone()),
            ),
            crate::feed_audit::AuditDisposition::Absent => {
                (crate::paper_recovery::FeedIncidentCause::Absence, None)
            }
            _ => return Ok(false),
        };
        let (acknowledged, response) = oneshot::channel();
        self.control_tx
            .send(OrchestratorControl::FeedAuditUpdate {
                update: crate::orchestrator_control::FeedAuditUpdate::Incident(
                    crate::paper_recovery::FeedIncident {
                        cause,
                        frame_receipt,
                        deciding_commitment_receipt,
                        counterpart_identity,
                        engagement_receipt: None,
                    },
                    verified_read,
                ),
                acknowledged,
            })
            .await
            .map_err(|_| ReconciliationError::ControlClosed)?;
        response
            .await
            .map_err(|_| ReconciliationError::ControlClosed)?
            .map_err(ReconciliationError::BucketCommit)?;
        Ok(true)
    }

    fn promote_selected_admissions(
        &self,
        wallet: WalletAddress,
        selected: &mut WalletObligations,
    ) -> Result<(), ReconciliationError> {
        for (_, epoch, obligation) in admitted_frame_obligations(&self.paper_state, Some(wallet))? {
            replace_admitted_obligation(selected, epoch, obligation, false);
        }
        Ok(())
    }

    fn disposed_obligations(
        &self,
        wallet: WalletAddress,
        selected: &WalletObligations,
        read: &crate::bucket_commit::VerifiedCommitment,
    ) -> Result<Vec<(i64, Obligation)>, ReconciliationError> {
        let fenced = self.paper_state.is_wallet_fenced(&wallet)?;
        let mut resolved = Vec::new();
        for (epoch, groups) in selected {
            for obligation in groups.values() {
                if let Some(frame) = self
                    .paper_state
                    .activity_frame_decision(&obligation.group_id)?
                    && frame.observed_source_receipt == Some(obligation.receipt)
                {
                    if let crate::feed_audit::AuditDisposition::Matched(id) =
                        crate::feed_audit::disposition(&frame, read)
                            .map_err(|error| ReconciliationError::Binding(error.to_string()))?
                        && let Some(binding) = read
                            .binding_indices
                            .get(&(obligation.receipt.sequence, obligation.receipt.this_hash))
                            .and_then(|index| read.bindings.get(*index))
                        && binding.history_group_id == id
                        && binding_target_disposed(&self.paper_state, binding)?
                    {
                        resolved.push((*epoch, obligation.clone()));
                    }
                    continue;
                }
                if obligation_disposed(&self.paper_state, fenced, obligation, false)? {
                    resolved.push((*epoch, obligation.clone()));
                }
            }
        }
        Ok(resolved)
    }

    #[allow(clippy::too_many_arguments)]
    fn correlate(
        &self,
        wallet: WalletAddress,
        selected: &WalletObligations,
        rows: &[NormalizedActivity],
        restamp_twins: &HashSet<SourceTradeId>,
        aggregates: &[&ActivityAggregate],
        identities: &[BucketIdentities],
        occurrences: &[PageOccurrence],
        pages: &[pe_source_polymarket_public::ReconciliationPageEvidence],
    ) -> Result<Correlation, ReconciliationError> {
        let mut result = Correlation::default();
        if selected.is_empty() {
            return Ok(result);
        }
        let pairs =
            crate::bucket_commit::read_restamp_pairs(rows).map_err(ActivityReadError::from)?;
        // A pair first seen together, neither stamp recorded, is one trade that bucket routing
        // would apply twice, so a feed observation of either stamp keeps the ambiguity fence.
        let mut first_seen = HashSet::new();
        for (restamp, original) in &pairs {
            if self.paper_state.activity_group_state(restamp)?.is_none()
                && self.paper_state.activity_group_state(original)?.is_none()
            {
                first_seen.extend([restamp.clone(), original.clone()]);
            }
        }
        // A restamp counts with its original only as a twin of a recorded original.
        let restamp_pairs = pairs
            .iter()
            .filter(|(restamp, _)| restamp_twins.contains(restamp))
            .map(|(restamp, original)| (restamp.clone(), original.clone()))
            .collect::<HashMap<_, _>>();
        let index = self.source_receipts.as_ref().ok_or_else(|| {
            ReconciliationError::Binding("source receipt index is absent".to_owned())
        })?;
        let by_group = aggregates
            .iter()
            .map(|aggregate| (aggregate.group_id.key(), *aggregate))
            .collect::<HashMap<_, _>>();
        let mut target_pages = HashMap::new();
        for (occurrence_index, (occurrence, evidence)) in joined_read_pages(occurrences, pages)
            .map_err(|error| ReconciliationError::Binding(error.to_string()))?
            .into_iter()
            .enumerate()
        {
            if pages.iter().any(|page| {
                page.bounds == evidence.bounds
                    && page.offset == pe_source_polymarket_public::ACTIVITY_MAX_OFFSET
                    && page.row_count == pe_source_polymarket_public::RECONCILIATION_PAGE_LIMIT
            }) {
                continue;
            }
            let envelope = index
                .source_envelope(occurrence.receipt)
                .map_err(|error| ReconciliationError::Binding(error.to_string()))?;
            let context = pe_source_polymarket_public::ActivityParseContext {
                source_id: envelope.source_id,
                observed_at: envelope.observed_at,
                received_at: envelope.received_at,
                transport: pe_source_polymarket_public::ActivityTransport::Rest,
            };
            let page = pe_source_polymarket_public::parse_activity_response(
                &envelope.payload,
                wallet,
                &context,
            )
            .map_err(|error| ReconciliationError::Binding(error.to_string()))?;
            for row in page.rows {
                let group = row
                    .group_id()
                    .map_err(|error| ReconciliationError::Binding(error.to_string()))?;
                target_pages
                    .entry(group.key().clone())
                    .or_insert(occurrence_index);
            }
        }
        for (epoch, groups) in selected {
            for obligation in groups.values() {
                let envelope = index
                    .source_envelope(obligation.receipt)
                    .map_err(|error| ReconciliationError::Binding(error.to_string()))?;
                if envelope.source_id.0 != ACTIVITY_WS_SOURCE_ID
                    || envelope.schema_version != ACTIVITY_SCHEMA_VERSION
                    || envelope.parser_version != ACTIVITY_PARSER_VERSION
                    || envelope.content_type != ContentType::Json
                {
                    return Err(ReconciliationError::Binding(
                        "stream source contract differs".to_owned(),
                    ));
                }
                let stream = parse_activity_trade_observation(&envelope.payload)
                    .map_err(|error| ReconciliationError::Binding(error.to_string()))?;
                if stream.wallet != wallet
                    || stream.group_id.key() != &obligation.group_id
                    || stream.source_time.0.unix_timestamp() != *epoch
                {
                    return Err(ReconciliationError::Binding(
                        "stream receipt differs from the frozen obligation".to_owned(),
                    ));
                }
                let original = stream.group_id.components();
                let frame_audit = obligation.frame_admission_receipt.is_some();
                let fixed_basis = frame_audit
                    .then(|| index.frame_counterpart_basis(obligation.receipt))
                    .flatten();
                let mut candidates = if frame_audit {
                    crate::feed_audit::resolve_frame_counterpart(
                        &stream,
                        fixed_basis.as_ref().map(|(target, _)| target.as_ref()),
                        aggregates.iter().copied(),
                        &pairs,
                    )
                } else if let Some(exact) = by_group.get(&obligation.group_id) {
                    vec![*exact]
                } else {
                    aggregates
                        .iter()
                        .copied()
                        .filter(|aggregate| {
                            let candidate = aggregate.group_id.components();
                            candidate.activity_type == original.activity_type
                                && candidate.wallet == original.wallet
                                && candidate.transaction_hash == original.transaction_hash
                                && candidate.asset == original.asset
                                && candidate.side == original.side
                        })
                        .collect::<Vec<_>>()
                };
                if !frame_audit {
                    crate::bucket_commit::collapse_restamp_pairs(&mut candidates, &restamp_pairs);
                }
                let provenance = if !frame_audit && by_group.contains_key(&obligation.group_id) {
                    None
                } else {
                    (if frame_audit {
                        candidates
                            .first()
                            .and_then(|target| target.group_id.components().asset.as_ref())
                    } else {
                        original.asset.as_ref()
                    })
                    .and_then(|asset| {
                        identities
                            .iter()
                            .find_map(|identities| identities.provenance.get(asset))
                    })
                    .cloned()
                };
                if !frame_audit
                    && !by_group.contains_key(&obligation.group_id)
                    && provenance.is_none()
                {
                    candidates.clear();
                }
                if candidates.len() > 1
                    || candidates
                        .iter()
                        .any(|candidate| first_seen.contains(candidate.group_id.key()))
                {
                    result
                        .ambiguous
                        .push((obligation.group_id.clone(), obligation.receipt));
                    continue;
                }
                let target = candidates.first().copied();
                let Some(target) = target else {
                    if frame_audit && index.frame_counterpart(obligation.receipt).is_some() {
                        continue;
                    }
                    result.unmatched.push((*epoch, obligation.group_id.clone()));
                    result.unmatched_epoch = Some(
                        result
                            .unmatched_epoch
                            .map_or(*epoch, |current| current.min(*epoch)),
                    );
                    continue;
                };
                let occurrence_index = *target_pages
                    .get(target.group_id.key())
                    .ok_or(ReconciliationError::PageReceiptMismatch)?;
                let identity_receipt = provenance
                    .as_ref()
                    .map(|proof| {
                        index
                            .receipt_at(pe_core_types::EventSeq(proof.source_log_sequence))
                            .map_err(|error| ReconciliationError::Binding(error.to_string()))?
                            .map(|(receipt, _)| receipt)
                            .ok_or_else(|| {
                                ReconciliationError::Binding(
                                    "metadata receipt is absent".to_owned(),
                                )
                            })
                    })
                    .transpose()?;
                result.matched.push(MatchedObservation {
                    qualifying_buy: original.side == Some(pe_core_types::Side::Buy)
                        && stream.share_amount != pe_core_types::ShareAmount::ZERO
                        && !stream.is_combo,
                    epoch: *epoch,
                    source_time: stream.source_time.0,
                    binding: ObservationBinding {
                        counterpart_basis_receipt: fixed_basis.map(|(_, receipt)| receipt),
                        frame_admission_receipt: obligation.frame_admission_receipt,
                        stream_group_id: obligation.group_id.clone(),
                        stream_receipt: obligation.receipt,
                        history_group_id: target.group_id.key().clone(),
                        semantic_revision: target.semantic_revision.as_str().to_owned(),
                        page_raw_hash: occurrences[occurrence_index].raw_hash.clone(),
                        page_occurrence_index: u32::try_from(occurrence_index)
                            .map_err(|_| ReconciliationError::PageReceiptMismatch)?,
                        identity_provenance: provenance,
                        identity_receipt,
                    },
                });
            }
        }
        Ok(result)
    }

    #[allow(clippy::too_many_arguments)]
    fn context(
        &self,
        fixed_end: i64,
        reconstruction_quality: ReconstructionQuality,
        copy_eligible: bool,
        pages: &[pe_source_polymarket_public::ReconciliationPageEvidence],
        page_occurrences: &[PageOccurrence],
        read_commitment: AppendReceipt,
        bucket: &[ActivityAggregate],
        identities: BucketIdentities,
        matched: &[MatchedObservation],
    ) -> Result<BucketDecisionContext, ReconciliationError> {
        let now = (self.now)();
        let mut observation_provenance = HashMap::new();
        let mut no_copy_dispositions = HashMap::new();
        let mut observed_source_receipts = HashMap::<SourceTradeId, AppendReceipt>::new();
        for aggregate in bucket {
            let group = aggregate.group_id.key().clone();
            let observation = matched
                .iter()
                .filter(|observation| observation.binding.history_group_id == group)
                .reduce(|existing, incoming| {
                    if crate::frame_admission::prefer_observation(
                        existing.binding.stream_receipt,
                        existing.binding.frame_admission_receipt.is_some(),
                        existing.qualifying_buy,
                        incoming.binding.stream_receipt,
                        incoming.binding.frame_admission_receipt.is_some(),
                        incoming.qualifying_buy,
                    ) {
                        incoming
                    } else {
                        existing
                    }
                });
            let provenance = observation
                .map(|_| TradeProvenance::ActivityWs)
                .unwrap_or(TradeProvenance::RestPoll);
            if let Some(observation) = observation {
                observed_source_receipts.insert(group.clone(), observation.binding.stream_receipt);
            }
            let source_time = crate::bucket_commit::earliest_bound_source_time(
                aggregate.source_time.0,
                matched
                    .iter()
                    .filter(|observation| observation.binding.history_group_id == group)
                    .map(|observation| observation.source_time),
            );
            observation_provenance.insert(group.clone(), provenance);
            if aggregate.group_id.components().activity_type == ActivityType::Trade
                && let Some(disposition) = stale_disposition(
                    provenance,
                    source_time,
                    now,
                    self.config.activity_ws_enabled,
                    self.config.copy_latency_budget_secs,
                )
            {
                no_copy_dispositions.insert(group, disposition);
            }
        }
        for group in &identities.unresolved {
            let provenance = observation_provenance
                .get(group)
                .copied()
                .unwrap_or(TradeProvenance::RestPoll);
            let source_time = bucket
                .iter()
                .find(|aggregate| aggregate.group_id.key() == group)
                .map(|aggregate| aggregate.source_time.0)
                .unwrap_or(now);
            no_copy_dispositions.insert(
                group.clone(),
                identity_unresolved_disposition(provenance, source_time, now),
            );
        }
        Ok(BucketDecisionContext {
            verified_read: None,
            applied_configuration: self.runtime_config.snapshot().as_ref().clone(),
            decision_inputs_json: serde_json::to_string(&serde_json::json!({
                "fixed_end": fixed_end,
                "pages": pages,
            }))?,
            page_occurrences: page_occurrences.to_vec(),
            observed_source_receipts,
            reconstruction_quality,
            read_commitment: Some(
                crate::bucket_commit::ActivityReadCommitmentReceipt::BindingsV2(read_commitment),
            ),
            signal_config: self.signal_config.clone(),
            copy_eligible,
            bracket_commit: false,
            recorded_at_unix: now.unix_timestamp(),
            observation_provenance,
            no_copy_dispositions,
            identity_overrides: identities.overrides,
            identity_unresolved: identities.unresolved,
            restamp_twins: HashSet::new(),
            history_status: None,
        })
    }

    /// Synchronize the commitment record binding one complete read (#565); the receipt is
    /// acknowledged only after the durable append, exactly like a page.
    async fn append_read_commitment(
        &self,
        wallet: WalletAddress,
        fixed_end: i64,
        page_occurrences: &[PageOccurrence],
        pages: &[pe_source_polymarket_public::ReconciliationPageEvidence],
        bindings: &[ObservationBinding],
        retain_proof: bool,
    ) -> Result<AppendReceipt, ReconciliationError> {
        let payload = activity_read_commitment_payload_v2(
            wallet,
            fixed_end,
            page_occurrences,
            pages,
            bindings,
        )
        .map_err(|_| ReconciliationError::PageReceiptMismatch)?;
        let payload = if retain_proof {
            let mut commitment: crate::bucket_commit::ActivityReadCommitment =
                serde_json::from_slice(&payload)?;
            commitment.read_proof = Some(crate::bucket_commit::CommittedReadProof {
                page_occurrences: page_occurrences.to_vec(),
                pages: pages.to_vec(),
            });
            serde_json::to_vec(&commitment)?
        } else {
            payload
        };
        let recorded_at = (self.now)();
        self.source_log
            .append(EnvelopeIn {
                source_id: SourceId(ACTIVITY_READ_COMMITMENT_SOURCE_ID.to_owned()),
                schema_version: ACTIVITY_READ_COMMITMENT_SCHEMA_VERSION,
                parser_version: ACTIVITY_READ_COMMITMENT_PARSER_VERSION,
                observed_at: SourceTimestamp(recorded_at),
                received_at: ReceivedAt(recorded_at),
                content_type: ContentType::Json,
                payload,
            })
            .await
            .map_err(|SourceLogHandleError::Closed| ReconciliationError::SourceLogClosed)
    }

    async fn resolve_bucket(
        &self,
        bucket: &[ActivityAggregate],
    ) -> Result<BucketIdentities, ReconciliationError> {
        let tokens = bucket
            .iter()
            .filter(|aggregate| !raw_only_combo(aggregate))
            .filter_map(|aggregate| aggregate.group_id.components().asset.clone())
            .collect::<HashSet<PolymarketTokenId>>();
        let resolved = self
            .asset_identity
            .resolve(tokens)
            .await
            .map_err(ReconciliationError::Identity)?;
        let mut identities = BucketIdentities {
            provenance: resolved.provenance,
            ..BucketIdentities::default()
        };
        for aggregate in bucket {
            if raw_only_combo(aggregate) {
                continue;
            }
            let components = aggregate.group_id.components();
            let Some(asset) = &components.asset else {
                continue;
            };
            let group = aggregate.group_id.key().clone();
            let Some(verified) = resolved.verified.get(asset) else {
                identities.unresolved.insert(group);
                continue;
            };
            let differs = components.condition_id.as_ref() != Some(&verified.condition_id)
                || components.outcome != Some(verified.outcome);
            if differs {
                identities.overrides.insert(
                    group,
                    IdentityOverride {
                        verified: MarketOutcomeId::new(
                            MarketId(VenueMarketId(verified.condition_id.0.clone())),
                            verified.outcome,
                        ),
                        evidence_hash: verified.evidence_hash.clone(),
                    },
                );
            }
        }
        Ok(identities)
    }

    async fn publish_feed_frontier(
        &self,
        wallet: WalletAddress,
        fixed_end: i64,
        commitment: AppendReceipt,
        page_occurrences: &[PageOccurrence],
        pages: &[pe_source_polymarket_public::ReconciliationPageEvidence],
        verified_read: Option<std::sync::Arc<crate::bucket_commit::VerifiedCommitment>>,
    ) -> Result<(), ReconciliationError> {
        let (acknowledged, received) = oneshot::channel();
        self.control_tx
            .send(OrchestratorControl::FeedAuditUpdate {
                update: crate::orchestrator_control::FeedAuditUpdate::Frontier(
                    crate::frame_admission::FeedHistoryFrontier {
                        version: 1,
                        wallet,
                        fixed_end,
                        commitment,
                        page_occurrences: page_occurrences.to_vec(),
                        pages: pages.to_vec(),
                    },
                    verified_read,
                ),
                acknowledged,
            })
            .await
            .map_err(|_| ReconciliationError::ControlClosed)?;
        received
            .await
            .map_err(|_| ReconciliationError::ControlClosed)?
            .map(|_| ())
            .map_err(ReconciliationError::BucketCommit)
    }

    async fn commit_bucket(
        &self,
        aggregates: Vec<ActivityAggregate>,
        context: BucketDecisionContext,
    ) -> Result<BucketCommitResult, ReconciliationError> {
        let (committed, result) = oneshot::channel();
        self.control_tx
            .send(OrchestratorControl::CommitActivityBucket {
                aggregates,
                context: Arc::new(context),
                committed,
            })
            .await
            .map_err(|_| ReconciliationError::ControlClosed)?;
        result
            .await
            .map_err(|_| ReconciliationError::ControlClosed)?
            .map_err(ReconciliationError::BucketCommit)
    }
}

fn raw_only_combo(aggregate: &ActivityAggregate) -> bool {
    aggregate.is_combo
        && matches!(
            aggregate.group_id.components().activity_type,
            ActivityType::Trade | ActivityType::Redeem
        )
}

/// Groups of this read that are restamps of a recorded original (#730 item 5), recorded
/// themselves or not: bucket routing applies the exemption to unseen ones, and feed correlation
/// counts any of them with its original.
fn restamp_twins(
    paper_state: &PaperStateDb,
    rows: &[NormalizedActivity],
) -> Result<HashSet<SourceTradeId>, ReconciliationError> {
    let mut members = HashMap::<SourceTradeId, Vec<&NormalizedActivity>>::new();
    for row in rows {
        let group = row.group_id().map_err(ActivityReadError::from)?;
        members.entry(group.key().clone()).or_default().push(row);
    }
    let mut twins = HashSet::new();
    for (group, members) in members {
        let Some(first) = members.first() else {
            continue;
        };
        let originals =
            crate::bucket_commit::unattributed_forms(&members).map_err(ActivityReadError::from)?;
        if originals.is_empty() {
            continue;
        }
        for original in originals {
            let Some(recorded) = paper_state.activity_group_state(original.group_id.key())? else {
                continue;
            };
            if recorded.semantic_revision != original.semantic_revision.as_str() {
                continue;
            }
            if first.activity_type == ActivityType::Trade {
                let effect = LedgerEffect::from_document(&recorded.proof_json)
                    .map_err(|error| ReconciliationError::Binding(error.to_string()))?;
                if matches!(effect, LedgerEffect::Corrected { .. })
                    && !matches!(effect.effective(), LedgerEffect::Trade { outcome_id, .. }
                        if Some(*outcome_id) == first.outcome)
                {
                    continue;
                }
            }
            twins.insert(group.clone());
        }
    }
    Ok(twins)
}

async fn wait_for_cancel(cancel: &mut watch::Receiver<bool>) {
    loop {
        if *cancel.borrow_and_update() || cancel.changed().await.is_err() {
            return;
        }
    }
}

#[derive(Default)]
struct Correlation {
    matched: Vec<MatchedObservation>,
    unmatched_epoch: Option<i64>,
    unmatched: Vec<(i64, SourceTradeId)>,
    ambiguous: Vec<(SourceTradeId, AppendReceipt)>,
}

struct MatchedObservation {
    qualifying_buy: bool,
    epoch: i64,
    source_time: OffsetDateTime,
    binding: ObservationBinding,
}

fn admitted_frame_obligations(
    state: &PaperStateDb,
    wallet: Option<WalletAddress>,
) -> Result<Vec<(WalletAddress, i64, Obligation)>, pe_paper_state::PaperStateError> {
    let mut result = Vec::new();
    for frame in state.activity_frame_decision_index(wallet.as_ref())? {
        let receipt = frame.observed_source_receipt.ok_or_else(|| {
            pe_paper_state::PaperStateError::Internal("frame receipt missing".to_owned())
        })?;
        result.push((
            frame.wallet,
            frame.source_epoch,
            Obligation {
                qualifying_buy: true,
                group_id: frame.source_trade_id,
                receipt,
                received_at: frame.received_at,
                bindings: Vec::new(),
                frame_admission_receipt: Some(frame.admission_receipt),
                retained_commitments: Vec::new(),
            },
        ));
    }
    Ok(result)
}

fn replace_admitted_obligation(
    selected: &mut WalletObligations,
    epoch: i64,
    obligation: Obligation,
    insert_missing: bool,
) {
    let present = selected
        .values()
        .any(|groups| groups.contains_key(&obligation.group_id.0));
    if !present && !insert_missing {
        return;
    }
    if let Some(existing) = selected
        .values()
        .find_map(|groups| groups.get(&obligation.group_id.0))
        && existing.receipt != obligation.receipt
        && !crate::frame_admission::prefer_observation(
            existing.receipt,
            existing.frame_admission_receipt.is_some(),
            existing.qualifying_buy,
            obligation.receipt,
            obligation.frame_admission_receipt.is_some(),
            obligation.qualifying_buy,
        )
    {
        return;
    }
    let mut bindings = Vec::new();
    let mut retained_commitments = Vec::new();
    for groups in selected.values_mut() {
        if let Some(existing) = groups.remove(&obligation.group_id.0) {
            retained_commitments.extend(existing.retained_commitments);
            bindings.extend(
                existing
                    .bindings
                    .into_iter()
                    .filter(|binding| binding.stream_receipt == obligation.receipt),
            );
        }
    }
    selected.retain(|_, groups| !groups.is_empty());
    selected.entry(epoch).or_default().insert(
        obligation.group_id.0.clone(),
        Obligation {
            bindings,
            retained_commitments,
            ..obligation
        },
    );
}

fn obligation_disposed(
    paper_state: &PaperStateDb,
    fenced: bool,
    obligation: &Obligation,
    authenticated_frame_match: bool,
) -> Result<bool, pe_paper_state::PaperStateError> {
    if let Some(frame) = paper_state.activity_frame_decision(&obligation.group_id)?
        && frame.observed_source_receipt == Some(obligation.receipt)
    {
        if !authenticated_frame_match {
            return Ok(false);
        }
        for binding in &obligation.bindings {
            if binding.stream_receipt == obligation.receipt
                && binding_target_disposed(paper_state, binding)?
            {
                return Ok(true);
            }
        }
        return Ok(false);
    }
    if obligation.bindings.is_empty() {
        return Ok(
            paper_state.activity_observation_unbound_retired(obligation.receipt)?
                || fenced
                || paper_state
                    .activity_group_state(&obligation.group_id)?
                    .is_some(),
        );
    }
    for binding in &obligation.bindings {
        if binding_target_disposed(paper_state, binding)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn binding_target_disposed(
    paper_state: &PaperStateDb,
    binding: &ObservationBinding,
) -> Result<bool, pe_paper_state::PaperStateError> {
    paper_state.activity_revision_disposed(&binding.history_group_id, &binding.semantic_revision)
}

fn select_refresh_candidate(
    candidates: &[(usize, AnchorRefreshClass)],
    reanchor_turn: &mut bool,
) -> Option<usize> {
    let preferred = if *reanchor_turn {
        AnchorRefreshClass::ReanchorRequired
    } else {
        AnchorRefreshClass::AgeDue
    };
    *reanchor_turn = !*reanchor_turn;
    candidates
        .iter()
        .find(|(_, class)| *class == preferred)
        .or_else(|| candidates.first())
        .map(|(index, _)| *index)
}

fn bucket_epoch(bucket: &[ActivityAggregate]) -> Result<i64, ReconciliationError> {
    bucket
        .first()
        .map(|aggregate| aggregate.source_time.0.unix_timestamp())
        .ok_or_else(|| ReconciliationError::BucketCommit("empty activity bucket".to_owned()))
}

fn stale_disposition(
    provenance: TradeProvenance,
    source_time: OffsetDateTime,
    now: OffsetDateTime,
    activity_ws_enabled: bool,
    copy_latency_budget_secs: u64,
) -> Option<NoCopyDisposition> {
    if !activity_ws_enabled {
        return None;
    }
    let budget =
        time::Duration::seconds(i64::try_from(copy_latency_budget_secs).unwrap_or(i64::MAX));
    let age = now - source_time;
    if age <= budget {
        return None;
    }
    let (provenance, reason) = match provenance {
        TradeProvenance::RestPoll => ("rest_poll", "stale_fallback_past_copy_budget"),
        TradeProvenance::ActivityWs => ("activity_ws", "stale_activity_ws_past_copy_budget"),
    };
    Some(NoCopyDisposition {
        provenance: provenance.to_owned(),
        age_secs: age.whole_seconds(),
        reason: reason.to_owned(),
        recorded_at_unix: now.unix_timestamp(),
    })
}

fn identity_unresolved_disposition(
    provenance: TradeProvenance,
    source_time: OffsetDateTime,
    now: OffsetDateTime,
) -> NoCopyDisposition {
    NoCopyDisposition {
        provenance: match provenance {
            TradeProvenance::RestPoll => "rest_poll",
            TradeProvenance::ActivityWs => "activity_ws",
        }
        .to_owned(),
        age_secs: (now - source_time).whole_seconds(),
        reason: "identity_unresolved".to_owned(),
        recorded_at_unix: now.unix_timestamp(),
    }
}

struct RecordingFetcher {
    now: Arc<dyn Fn() -> OffsetDateTime + Send + Sync>,
    inner: Arc<dyn ReconciliationFetcher>,
    source_log: SourceLogHandle,
    append_closed: Arc<AtomicBool>,
    occurrences: Arc<Mutex<Vec<PageOccurrence>>>,
}

impl RecordingFetcher {
    fn join_occurrences(
        &self,
        pages: &[pe_source_polymarket_public::ReconciliationPageEvidence],
    ) -> Result<Vec<PageOccurrence>, ReconciliationError> {
        let occurrences = self
            .occurrences
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        joined_read_pages(&occurrences, pages)
            .map_err(|_| ReconciliationError::PageReceiptMismatch)?;
        Ok(occurrences.clone())
    }
}

impl ReconciliationFetcher for RecordingFetcher {
    fn fetch<'a>(
        &'a self,
        url: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, SourceError>> + Send + 'a>> {
        Box::pin(async move {
            let payload = self.inner.fetch(url).await?;
            let received_at = (self.now)();
            let envelope = EnvelopeIn {
                source_id: SourceId(ACTIVITY_POLL_SOURCE_ID.to_owned()),
                schema_version: ACTIVITY_POLL_PAGE_SCHEMA_VERSION,
                parser_version: ACTIVITY_PARSER_VERSION,
                observed_at: SourceTimestamp(received_at),
                received_at: ReceivedAt(received_at),
                content_type: ContentType::Json,
                payload: payload.clone(),
            };
            let receipt = self.source_log.append(envelope).await.map_err(
                |SourceLogHandleError::Closed| {
                    self.append_closed.store(true, Ordering::Release);
                    SourceError::Fatal {
                        message: "source-log coordinator closed".to_owned(),
                    }
                },
            )?;
            self.occurrences
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(PageOccurrence {
                    request_url: url.to_owned(),
                    raw_hash: blake3::hash(&payload).to_hex().to_string(),
                    receipt,
                });
            Ok(payload)
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use pe_core_types::{CollateralAmount, OutcomeId};
    use pe_event_log::Writer;
    use pe_paper_state::{ActivityBucketCommit, ActivityDispositionRecord};
    use pe_source_polymarket_public::aggregate_activity_rows;
    use tempfile::tempdir;

    use crate::paper_recovery::{
        PAPER_LOG_SCHEMA_VERSION, PaperLogRecord, PortfolioMark, QualificationStarted, TailBinding,
    };

    #[test]
    fn missing_attempt_is_ready_in_the_same_second_until_its_deadline() {
        let now = OffsetDateTime::from_unix_timestamp(100).unwrap();
        let mut attempt = WalletAttempt::default();
        assert!(attempt.ready(now));
        attempt.last_fixed_end = Some(100);
        attempt.deadline = Some(now + time::Duration::milliseconds(500));
        assert!(attempt.ready(now));
        assert!(attempt.ready(now + time::Duration::milliseconds(500)));
        assert!(!attempt.ready(now + time::Duration::milliseconds(501)));
        attempt.deadline = None;
        assert!(!attempt.ready(now));
    }

    fn restamp_row(activity_type: &str) -> serde_json::Value {
        serde_json::json!({
            "proxyWallet": "0x50b4ab8658dd8c9901c69208d275d81cdeaac879",
            "timestamp": 1790733191,
            "conditionId": "0xce5c9d4d7b3e8c391c67306fc17071e36d44af7d35e8ec024d075e23e7b10a46",
            "type": activity_type,
            "size": "11.21", "usdcSize": "11.19879",
            "transactionHash": "0x0bf3367f2a5a71e2edddc893acc5fd3204a6f6b6e7965ccd287f0eb9f001f134",
            "price": "0.999",
            "asset": "28453451086353907670219618525088571078287035298566106984967643506070520453709",
            "side": "BUY", "outcomeIndex": 0, "outcome": "Canadiens",
        })
    }

    fn restamp_rows(rows: &[serde_json::Value]) -> Vec<NormalizedActivity> {
        let wallet = WalletAddress::from_hex(rows[0]["proxyWallet"].as_str().unwrap()).unwrap();
        pe_source_polymarket_public::parse_activity_response(
            &serde_json::to_vec(rows).unwrap(),
            wallet,
            &pe_source_polymarket_public::ActivityParseContext {
                source_id: SourceId("restamp-fixture".to_owned()),
                observed_at: SourceTimestamp(OffsetDateTime::UNIX_EPOCH),
                received_at: ReceivedAt(OffsetDateTime::UNIX_EPOCH),
                transport: pe_source_polymarket_public::ActivityTransport::Rest,
            },
        )
        .unwrap()
        .rows
    }

    fn record_restamp_original(state: &PaperStateDb, rows: &[NormalizedActivity], outcome: u16) {
        let aggregate = aggregate_activity_rows(rows).unwrap().remove(0);
        let components = aggregate.group_id.components();
        let effect = pe_position_ledger::LedgerMutation::from_activity(&aggregate)
            .unwrap()
            .with_verified_identity(
                MarketOutcomeId::new(
                    MarketId(VenueMarketId(
                        components.condition_id.as_ref().unwrap().0.clone(),
                    )),
                    OutcomeId(outcome),
                ),
                "recorded-gamma-page".to_owned(),
            )
            .effect;
        state
            .commit_activity_bucket(&ActivityBucketCommit {
                wallet: components.wallet,
                source_epoch: aggregate.source_time.0.unix_timestamp(),
                dispositions: vec![ActivityDispositionRecord {
                    source_trade_id: aggregate.group_id.key().clone(),
                    transaction_hash: components.transaction_hash.clone(),
                    wallet: components.wallet,
                    source_epoch: aggregate.source_time.0.unix_timestamp(),
                    semantic_revision: aggregate.semantic_revision.as_str().to_owned(),
                    activity_type: components.activity_type.as_str().to_owned(),
                    disposition: "not_an_entry".to_owned(),
                    proof_json: effect.to_document().unwrap(),
                    no_copy: None,
                }],
                leader_positions: Vec::new(),
                gate_results: Vec::new(),
                history_effects: Vec::new(),
                history_status: None,
                pending: Vec::new(),
                fence: None,
                reanchor: None,
                advance_cursor: true,
            })
            .unwrap();
    }

    #[test]
    fn restamp_twins_reproduce_production_trade_and_redeem_hashes() {
        let dir = tempdir().unwrap();
        let state = PaperStateDb::open(&dir.path().join("paper.db")).unwrap();
        let trade = restamp_row("TRADE");
        let mut stamped = trade.clone();
        stamped["outcomeIndex"] = serde_json::json!(999);
        let originals = restamp_rows(&[stamped]);
        let original = aggregate_activity_rows(&originals).unwrap().remove(0);
        assert_eq!(
            original.group_id.key().0,
            "g2:73e4d6ccac6eab6e0a314c987ac0a35e1ceb92c649855cce23f80474553578ca"
        );
        assert_eq!(
            original.semantic_revision.as_str(),
            "74d437c773eb6e214195fb4113f6f729ee1d7e1be63ae9912b54a820264bbd9e"
        );
        let current = restamp_rows(&[trade]);
        let twin = aggregate_activity_rows(&current).unwrap().remove(0);
        assert_eq!(
            twin.group_id.key().0,
            "g2:778a6151361b1edb5ce1db9b9e915c5d812b7c5303ff5a67522063101988af0b"
        );
        assert_eq!(
            twin.semantic_revision.as_str(),
            "f1c21e323b3f2c79bb096edc39cc61bc16a748251dde06f2ffbc5c22f9554a1c"
        );
        assert!(restamp_twins(&state, &current).unwrap().is_empty());
        record_restamp_original(&state, &originals, 0);
        assert_eq!(
            restamp_twins(&state, &current).unwrap(),
            HashSet::from([twin.group_id.key().clone()])
        );

        let redeem = serde_json::json!({
            "proxyWallet": "0x9219dd565d7521e95f273b7eea68dbb08d40027c",
            "timestamp": 1790556656,
            "conditionId": "0x0fa5e3bb262d1b272645648ae43a167a50524ded1282c31e5c637797039a4327",
            "type": "REDEEM", "size": "5", "usdcSize": "5", "price": "0",
            "transactionHash": "0x953ce04ae860f354b0aef3fa738c552bbb3880874d07ddc02af87d07c3eb1e17",
            "asset": "", "side": "", "outcomeIndex": 0, "outcome": "Over",
        });
        let mut stamped = redeem.clone();
        stamped["outcomeIndex"] = serde_json::json!(999);
        stamped["outcome"] = serde_json::json!("");
        let originals = restamp_rows(&[stamped]);
        assert_eq!(originals[0].outcome, None);
        let original = aggregate_activity_rows(&originals).unwrap().remove(0);
        assert_eq!(
            original.group_id.key().0,
            "g2:7dcdf42e127ea0c2384dcdecb1a5276ecfae9f9770fc53935919824161d43e00"
        );
        assert_eq!(
            original.semantic_revision.as_str(),
            "d9bd9c1ef6b013351374ba5629fd58f3cb9db0f2621081592afcdcd9944b697c"
        );
        record_restamp_original(&state, &originals, 0);
        let current = restamp_rows(&[redeem]);
        let twin = aggregate_activity_rows(&current).unwrap().remove(0);
        assert_eq!(
            twin.group_id.key().0,
            "g2:4792cbb3b2194867bfcdd2a22501e7ea4d7299fb18d060526cc191647309be16"
        );
        assert_eq!(
            restamp_twins(&state, &current).unwrap(),
            HashSet::from([twin.group_id.key().clone()])
        );
    }

    #[test]
    fn restamp_twins_require_all_members_and_matching_corrected_outcome() {
        for activity_type in ["TRADE", "REDEEM"] {
            for count in [1, 3] {
                let dir = tempdir().unwrap();
                let state = PaperStateDb::open(&dir.path().join("paper.db")).unwrap();
                let mut current = restamp_row(activity_type);
                if activity_type == "REDEEM" {
                    current["asset"] = serde_json::json!("");
                    current["side"] = serde_json::json!("");
                }
                let mut stamped = current.clone();
                stamped["outcomeIndex"] = serde_json::json!(999);
                if activity_type == "REDEEM" {
                    stamped["outcome"] = serde_json::json!("");
                }
                let originals = restamp_rows(&vec![stamped; count]);
                record_restamp_original(&state, &originals, 0);
                let current_rows = restamp_rows(&vec![current.clone(); count]);
                assert_eq!(restamp_twins(&state, &current_rows).unwrap().len(), 1);
                let mut changed = current.clone();
                changed["size"] = serde_json::json!("11.22");
                assert!(
                    restamp_twins(&state, &restamp_rows(&vec![changed; count]))
                        .unwrap()
                        .is_empty()
                );
                assert!(
                    restamp_twins(&state, &restamp_rows(&vec![current.clone(); count + 1]))
                        .unwrap()
                        .is_empty()
                );
                if activity_type == "TRADE" {
                    current["outcomeIndex"] = serde_json::json!(1);
                    assert!(
                        restamp_twins(&state, &restamp_rows(&vec![current; count]))
                            .unwrap()
                            .is_empty()
                    );
                }
            }
        }
    }

    #[test]
    fn read_restamp_pairs_count_one_trade_and_keep_distinct_legs() {
        let key = |rows: &[serde_json::Value]| {
            aggregate_activity_rows(&restamp_rows(rows))
                .unwrap()
                .remove(0)
                .group_id
                .key()
                .clone()
        };
        let pairs = |rows: &[serde_json::Value]| {
            crate::bucket_commit::read_restamp_pairs(&restamp_rows(rows)).unwrap()
        };
        for activity_type in ["TRADE", "REDEEM"] {
            for count in [1, 3] {
                let mut current = restamp_row(activity_type);
                if activity_type == "REDEEM" {
                    current["asset"] = serde_json::json!("");
                    current["side"] = serde_json::json!("");
                }
                let mut stamped = current.clone();
                stamped["outcomeIndex"] = serde_json::json!(999);
                if activity_type == "REDEEM" {
                    stamped["outcome"] = serde_json::json!("");
                }
                let originals = vec![stamped.clone(); count];
                let restamps = vec![current.clone(); count];
                assert_eq!(
                    pairs(&[originals.clone(), restamps.clone()].concat()),
                    HashMap::from([(key(&restamps), key(&originals))])
                );
                // A missing or extra member row, or a changed size, is a distinct leg.
                let extra = vec![current.clone(); count + 1];
                assert!(pairs(&[originals.clone(), extra].concat()).is_empty());
                let mut changed = current.clone();
                changed["size"] = serde_json::json!("11.22");
                assert!(pairs(&[originals.clone(), vec![changed; count]].concat()).is_empty());
            }
        }

        let current = restamp_row("TRADE");
        let mut stamped = current.clone();
        stamped["outcomeIndex"] = serde_json::json!(999);
        let mut other = current.clone();
        other["outcomeIndex"] = serde_json::json!(1);
        assert_eq!(
            pairs(&[stamped.clone(), other.clone()]),
            HashMap::from([(
                key(std::slice::from_ref(&other)),
                key(std::slice::from_ref(&stamped))
            )])
        );
        // Two groups reproducing one original cannot both be its restamp.
        let read = restamp_rows(&[stamped.clone(), current.clone(), other.clone()]);
        assert!(
            crate::bucket_commit::read_restamp_pairs(&read)
                .unwrap()
                .is_empty()
        );
        let read = restamp_rows(&[stamped.clone(), current.clone()]);
        let pairs = crate::bucket_commit::read_restamp_pairs(&read).unwrap();
        let aggregates = aggregate_activity_rows(&read).unwrap();
        let collapsed = |rows: &[&serde_json::Value]| {
            let mut candidates = rows
                .iter()
                .map(|row| {
                    let id = key(std::slice::from_ref(*row));
                    aggregates
                        .iter()
                        .find(|aggregate| aggregate.group_id.key() == &id)
                        .unwrap()
                })
                .collect::<Vec<_>>();
            crate::bucket_commit::collapse_restamp_pairs(&mut candidates, &pairs);
            candidates
                .iter()
                .map(|aggregate| aggregate.group_id.key().clone())
                .collect::<Vec<_>>()
        };
        assert_eq!(
            collapsed(&[&stamped, &current]),
            vec![key(std::slice::from_ref(&stamped))]
        );
        assert_eq!(
            collapsed(&[&current, &stamped]),
            vec![key(std::slice::from_ref(&stamped))]
        );
        assert_eq!(
            collapsed(&[&current]),
            vec![key(std::slice::from_ref(&current))]
        );
    }

    #[test]
    fn combo_identity_exemption_preserves_conversion_and_unknown_precedence() {
        for activity_type in ["TRADE", "REDEEM", "CONVERSION", "FUTURE"] {
            let mut row = restamp_row(activity_type);
            row["isCombo"] = serde_json::json!(true);
            let aggregate = aggregate_activity_rows(&restamp_rows(&[row]))
                .unwrap()
                .remove(0);
            assert_eq!(
                raw_only_combo(&aggregate),
                matches!(activity_type, "TRADE" | "REDEEM")
            );
        }
    }

    #[test]
    fn poller_stale_gate_uses_full_120_second_window_for_both_provenances() {
        let source = OffsetDateTime::from_unix_timestamp(1_800_000_000).unwrap();
        let deadline = source + time::Duration::seconds(120);
        for provenance in [TradeProvenance::RestPoll, TradeProvenance::ActivityWs] {
            assert!(stale_disposition(provenance, source, deadline, true, 120).is_none());
            let expired = stale_disposition(
                provenance,
                source,
                deadline + time::Duration::nanoseconds(1),
                true,
                120,
            )
            .unwrap();
            assert_eq!(expired.age_secs, 120);
            assert!(
                stale_disposition(
                    provenance,
                    source,
                    deadline + time::Duration::seconds(1),
                    false,
                    120
                )
                .is_none()
            );
        }
    }

    fn recorded(sequence: u64, url: &str, hash: &str) -> PageOccurrence {
        PageOccurrence {
            request_url: url.to_owned(),
            raw_hash: hash.to_owned(),
            receipt: AppendReceipt {
                sequence: pe_core_types::EventSeq(sequence),
                this_hash: blake3::Hash::from_bytes([u8::try_from(sequence).unwrap_or(0); 32]),
            },
        }
    }

    fn evidence(
        url: &str,
        hash: &str,
        end: i64,
    ) -> pe_source_polymarket_public::ReconciliationPageEvidence {
        pe_source_polymarket_public::ReconciliationPageEvidence {
            request_url: url.to_owned(),
            bounds: Some(pe_source_polymarket_public::ActivityRequestBounds {
                start: Some(end - 1),
                end,
            }),
            partition: None,
            offset: 0,
            row_count: 0,
            canonical_page_hash: format!("canonical-{end}"),
            raw_page_hash: hash.to_owned(),
            received_at: ReceivedAt(OffsetDateTime::UNIX_EPOCH),
            schema_version: ACTIVITY_SCHEMA_VERSION,
            parser_version: ACTIVITY_PARSER_VERSION,
        }
    }

    fn append_source_frame(
        writer: &mut Writer,
        source_id: &str,
        payload: &[u8],
        received_at_unix: i64,
        schema_version: u32,
        parser_version: u32,
    ) -> AppendReceipt {
        let timestamp = OffsetDateTime::from_unix_timestamp(received_at_unix).unwrap();
        writer
            .append_synced(EnvelopeIn {
                source_id: SourceId(source_id.to_owned()),
                schema_version,
                parser_version,
                observed_at: SourceTimestamp(timestamp),
                received_at: ReceivedAt(timestamp),
                content_type: ContentType::Json,
                payload: payload.to_vec(),
            })
            .unwrap()
    }

    fn activity_payload(transaction_suffix: char, source_unix: i64) -> Vec<u8> {
        format!(
            r#"{{"proxyWallet":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","conditionId":"0xc1","asset":"123","side":"BUY","size":"5","price":"0.5","timestamp":{source_unix},"transactionHash":"0x{}","outcomeIndex":"0"}}"#,
            transaction_suffix.to_string().repeat(64)
        )
        .into_bytes()
    }

    fn resolve_activity_group(paper_state: &PaperStateDb, payload: &[u8]) {
        let activity = parse_activity_trade_observation(payload).unwrap();
        paper_state
            .commit_activity_bucket(&ActivityBucketCommit {
                wallet: activity.wallet,
                source_epoch: activity.source_time.0.unix_timestamp(),
                dispositions: vec![ActivityDispositionRecord {
                    source_trade_id: activity.group_id.key().clone(),
                    transaction_hash: activity.group_id.components().transaction_hash.clone(),
                    wallet: activity.wallet,
                    source_epoch: activity.source_time.0.unix_timestamp(),
                    semantic_revision: "candidate-split-test-v1".to_owned(),
                    activity_type: "TRADE".to_owned(),
                    disposition: "decision_pending".to_owned(),
                    proof_json: "{\"version\":1}".to_owned(),
                    no_copy: None,
                }],
                leader_positions: Vec::new(),
                gate_results: Vec::new(),
                history_effects: Vec::new(),
                history_status: None,
                pending: Vec::new(),
                fence: None,
                reanchor: None,
                advance_cursor: false,
            })
            .unwrap();
    }

    fn empty_tail() -> TailBinding {
        TailBinding {
            physical_tail: 0,
            last_sequence: None,
            last_hash: blake3::Hash::from_bytes([0; 32]).to_hex().to_string(),
        }
    }

    fn qualification_start(wallet: WalletAddress) -> PaperLogRecord {
        PaperLogRecord::QualificationStarted(Arc::new(QualificationStarted {
            starting_bankroll: CollateralAmount::from_atomic(100_000_000),
            paper_prefix: empty_tail(),
            source_prefix: empty_tail(),
            live_prefix: empty_tail(),
            artifact_blake3: "artifact".to_owned(),
            static_config_hash: "static".to_owned(),
            hot_config_hash: "hot".to_owned(),
            generation: "generation".to_owned(),
            activation_id: "activation".to_owned(),
            ranking_batch_id: 572,
            membership: vec![wallet],
            membership_proofs_hash: "membership".to_owned(),
            schema_version: 3,
            parser_version: 1,
            financial_semantic_version: 1,
        }))
    }

    fn append_paper_record(
        writer: &mut Writer,
        record: &PaperLogRecord,
        received_at_unix: i64,
    ) -> AppendReceipt {
        let timestamp = OffsetDateTime::from_unix_timestamp(received_at_unix).unwrap();
        writer
            .append_synced(EnvelopeIn {
                source_id: SourceId("pe-service.paper".to_owned()),
                schema_version: PAPER_LOG_SCHEMA_VERSION,
                parser_version: 1,
                observed_at: SourceTimestamp(timestamp),
                received_at: ReceivedAt(timestamp),
                content_type: ContentType::Json,
                payload: serde_json::to_vec(record).unwrap(),
            })
            .unwrap()
    }

    /// PASS: logically sorted saturated pages join by multiplicity while receipt output retains
    /// acquisition order, including repeated identical request/hash pairs.
    #[test]
    fn page_receipts_join_as_a_fifo_multiset() {
        let acquired = vec![
            recorded(1, "parent", "p"),
            recorded(2, "child", "same"),
            recorded(3, "child", "same"),
        ];
        let logical = vec![
            evidence("child", "same", 1),
            evidence("child", "same", 2),
            evidence("parent", "p", 3),
        ];
        let joined = joined_read_pages(&acquired, &logical).unwrap();
        assert_eq!(
            joined
                .iter()
                .map(|(page, _)| page.receipt.sequence.0)
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        let missing = vec![evidence("child", "same", 1), evidence("parent", "p", 2)];
        assert!(joined_read_pages(&acquired, &missing).is_err());
    }

    #[test]
    fn refresh_selection_alternates_due_classes_and_falls_through() {
        let candidates = [
            (0, AnchorRefreshClass::ReanchorRequired),
            (1, AnchorRefreshClass::AgeDue),
            (2, AnchorRefreshClass::ReanchorRequired),
            (3, AnchorRefreshClass::AgeDue),
        ];
        let mut reanchor_turn = true;
        assert_eq!(
            select_refresh_candidate(&candidates, &mut reanchor_turn),
            Some(0)
        );
        assert_eq!(
            select_refresh_candidate(&candidates[1..], &mut reanchor_turn),
            Some(1)
        );
        assert_eq!(
            select_refresh_candidate(&candidates[2..], &mut reanchor_turn),
            Some(2)
        );
        assert_eq!(
            select_refresh_candidate(&candidates[3..], &mut reanchor_turn),
            Some(3)
        );

        let only_age_due = [(7, AnchorRefreshClass::AgeDue)];
        assert_eq!(
            select_refresh_candidate(&only_age_due, &mut reanchor_turn),
            Some(7),
            "reanchor turn falls through to age-due"
        );
        assert!(!reanchor_turn, "the class turn still advances");
        assert_eq!(select_refresh_candidate(&[], &mut reanchor_turn), None);
        let only_reanchor = [(9, AnchorRefreshClass::ReanchorRequired)];
        assert_eq!(
            select_refresh_candidate(&only_reanchor, &mut reanchor_turn),
            Some(9)
        );
    }

    #[test]
    fn strict_copy_budget_boundary_preserves_both_sides() {
        for (stream_epoch, history_epoch) in [(100, 101), (101, 100)] {
            let stream = OffsetDateTime::from_unix_timestamp(stream_epoch).unwrap();
            let history = OffsetDateTime::from_unix_timestamp(history_epoch).unwrap();
            let oldest = stream.min(history);
            let boundary = oldest + time::Duration::seconds(2);
            assert!(
                stale_disposition(TradeProvenance::ActivityWs, oldest, boundary, true, 2).is_none()
            );
            assert!(
                stale_disposition(
                    TradeProvenance::ActivityWs,
                    oldest,
                    boundary + time::Duration::nanoseconds(1),
                    true,
                    2
                )
                .is_some()
            );
        }
        let source = OffsetDateTime::from_unix_timestamp(100).unwrap();
        assert!(
            stale_disposition(
                TradeProvenance::ActivityWs,
                source,
                source + time::Duration::seconds(1),
                true,
                2,
            )
            .is_none()
        );
        assert!(
            stale_disposition(
                TradeProvenance::ActivityWs,
                source,
                source + time::Duration::seconds(2),
                true,
                2,
            )
            .is_none()
        );
        let disposition = stale_disposition(
            TradeProvenance::ActivityWs,
            source,
            source + time::Duration::seconds(2) + time::Duration::nanoseconds(1),
            true,
            2,
        )
        .unwrap();
        assert_eq!(disposition.reason, "stale_activity_ws_past_copy_budget");
        assert_eq!(disposition.age_secs, 2);
    }

    #[test]
    fn obligations_coalesce_three_readers_by_wallet_group() {
        let wallet = WalletAddress::from_hex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        let source_time = OffsetDateTime::from_unix_timestamp(100).unwrap();
        let mut obligations = ReconciliationObligations::default();
        for received in [103, 101, 102] {
            obligations.insert(ReconciliationTrigger {
                qualifying_buy: true,
                wallet,
                source_time,
                source_trade_id: SourceTradeId("g2:a".to_owned()),
                provenance: TradeProvenance::ActivityWs,
                received_at: OffsetDateTime::from_unix_timestamp(received).unwrap(),
                receipt: pe_event_log::AppendReceipt {
                    sequence: pe_core_types::EventSeq(u64::try_from(received).unwrap()),
                    this_hash: blake3::Hash::from_bytes([u8::try_from(received).unwrap_or(0); 32]),
                },
            });
        }
        assert_eq!(obligations.len(), 1);
        assert_eq!(obligations.earliest_epoch(&wallet), Some(100));
        let observation = obligations
            .observation(&wallet, 100, &SourceTradeId("g2:a".to_owned()))
            .unwrap();
        assert_eq!(observation.0.sequence, pe_core_types::EventSeq(101));
        assert_eq!(observation.1.unix_timestamp(), 101);
        let selected = obligations.by_wallet.get(&wallet).unwrap().clone();
        let snapshot = selected.clone();
        obligations.insert(ReconciliationTrigger {
            qualifying_buy: true,
            wallet,
            source_time: OffsetDateTime::from_unix_timestamp(101).unwrap(),
            source_trade_id: SourceTradeId("later-group".to_owned()),
            provenance: TradeProvenance::ActivityWs,
            received_at: OffsetDateTime::from_unix_timestamp(102).unwrap(),
            receipt: recorded(99, "later", "later").receipt,
        });
        assert_eq!(
            selected, snapshot,
            "later readers do not mutate a frozen attempt"
        );
        assert_eq!(obligations.len(), 2);
    }

    /// PASS: only an obligation received before the cutoff and appended inside the boundary prefix
    /// delays the boundary; either crossing order belongs to the later interval.
    #[test]
    fn boundary_uses_both_receipt_and_receive_time_bounds() {
        let wallet = WalletAddress::from_hex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        let source_time = OffsetDateTime::from_unix_timestamp(900).unwrap();
        let trigger = |id: &str, sequence: u64, received_at: i64| ReconciliationTrigger {
            qualifying_buy: true,
            wallet,
            source_time,
            source_trade_id: SourceTradeId(id.to_owned()),
            provenance: TradeProvenance::ActivityWs,
            received_at: OffsetDateTime::from_unix_timestamp(received_at).unwrap(),
            receipt: AppendReceipt {
                sequence: pe_core_types::EventSeq(sequence),
                this_hash: blake3::Hash::from_bytes([u8::try_from(sequence).unwrap_or(0); 32]),
            },
        };
        let mut obligations = ReconciliationObligations::default();
        assert!(obligations.install_boundary(PendingBoundary {
            cutoff_unix: 1_000,
            receipt: trigger("boundary", 10, 1_000).receipt,
        }));
        assert!(!obligations.install_boundary(PendingBoundary {
            cutoff_unix: 2_000,
            receipt: trigger("later", 20, 2_000).receipt,
        }));
        obligations.insert(trigger("post-receive", 9, 1_001));
        obligations.insert(trigger("late-append", 11, 999));
        assert!(obligations.boundary_ready());

        obligations.insert(trigger("qualifying", 8, 999));
        assert!(!obligations.boundary_ready());
        obligations.remove(&wallet, 900, &SourceTradeId("qualifying".to_owned()));
        assert_eq!(
            obligations.take_ready_boundary().unwrap().cutoff_unix,
            1_000
        );
    }

    /// PASS: restart/quiet-day catch-up yields every completed midnight after Start in oldest-first
    /// order and never repeats an existing boundary cutoff.
    #[test]
    fn completed_midnights_catch_up_oldest_first() {
        assert_eq!(
            completed_midnight_cutoffs_after(100, 3 * SECONDS_PER_DAY + 1).unwrap(),
            vec![SECONDS_PER_DAY, 2 * SECONDS_PER_DAY, 3 * SECONDS_PER_DAY]
        );
        assert_eq!(
            completed_midnight_cutoffs_after(SECONDS_PER_DAY, 2 * SECONDS_PER_DAY).unwrap(),
            vec![2 * SECONDS_PER_DAY]
        );
        assert!(
            completed_midnight_cutoffs_after(100, 99)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn activity_candidates_filter_after_database_changes_and_match_rebuild() {
        let dir = tempdir().unwrap();
        let source_path = dir.path().join("source.log");
        let paper_state = PaperStateDb::open(&dir.path().join("paper.db")).unwrap();
        let resolved_payload = activity_payload('a', 100);
        let unresolved_payload = activity_payload('b', 101);
        let suffix_payload = activity_payload('c', 102);
        let unresolved_group = parse_activity_trade_observation(&unresolved_payload)
            .unwrap()
            .group_id
            .key()
            .clone();
        let suffix_group = parse_activity_trade_observation(&suffix_payload)
            .unwrap()
            .group_id
            .key()
            .clone();

        let mut writer = Writer::open(&source_path).unwrap();
        append_source_frame(
            &mut writer,
            ACTIVITY_WS_SOURCE_ID,
            &resolved_payload,
            110,
            ACTIVITY_SCHEMA_VERSION,
            ACTIVITY_PARSER_VERSION,
        );
        let first_unresolved = append_source_frame(
            &mut writer,
            ACTIVITY_WS_SOURCE_ID,
            &unresolved_payload,
            111,
            ACTIVITY_SCHEMA_VERSION,
            ACTIVITY_PARSER_VERSION,
        );
        append_source_frame(
            &mut writer,
            ACTIVITY_WS_SOURCE_ID,
            &unresolved_payload,
            112,
            ACTIVITY_SCHEMA_VERSION,
            ACTIVITY_PARSER_VERSION,
        );

        let initial = Reader::replay(&source_path)
            .unwrap()
            .map(|item| item.unwrap().1)
            .collect::<Vec<_>>();
        let mut candidates = ActivityCandidates::default();
        for envelope in &initial {
            candidates.observe_activity(envelope).unwrap();
        }
        resolve_activity_group(&paper_state, &resolved_payload);

        let suffix_receipt = append_source_frame(
            &mut writer,
            ACTIVITY_WS_SOURCE_ID,
            &suffix_payload,
            113,
            ACTIVITY_SCHEMA_VERSION,
            ACTIVITY_PARSER_VERSION,
        );
        let suffix = Reader::replay(&source_path)
            .unwrap()
            .nth(initial.len())
            .unwrap()
            .unwrap()
            .1;
        candidates.observe_activity(&suffix).unwrap();
        drop(writer);

        let split = candidates
            .into_obligations(
                &paper_state,
                &SourceReceiptIndex::replay(&source_path).unwrap(),
            )
            .unwrap();
        let rebuilt = rebuild_reconciliation_obligations(&source_path, &paper_state).unwrap();
        assert_eq!(split, rebuilt);
        assert_eq!(split.len(), 2);
        assert_eq!(
            split
                .observation(
                    &WalletAddress::from_hex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap(),
                    101,
                    &unresolved_group,
                )
                .unwrap()
                .0,
            first_unresolved,
            "duplicate observations retain their earliest source receipt"
        );
        assert_eq!(
            split
                .observation(
                    &WalletAddress::from_hex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap(),
                    102,
                    &suffix_group,
                )
                .unwrap()
                .0,
            suffix_receipt,
            "a later-observed log suffix remains after the final state filter"
        );
    }

    #[test]
    fn malformed_activity_candidate_matches_rebuild_error() {
        let dir = tempdir().unwrap();
        let source_path = dir.path().join("source.log");
        let paper_state = PaperStateDb::open(&dir.path().join("paper.db")).unwrap();
        let mut writer = Writer::open(&source_path).unwrap();
        append_source_frame(
            &mut writer,
            ACTIVITY_WS_SOURCE_ID,
            b"{",
            100,
            ACTIVITY_SCHEMA_VERSION,
            ACTIVITY_PARSER_VERSION,
        );
        drop(writer);
        let envelope = Reader::replay(&source_path)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .1;
        let observed = ActivityCandidates::default()
            .observe_activity(&envelope)
            .unwrap_err();
        let rebuilt = rebuild_reconciliation_obligations(&source_path, &paper_state).unwrap_err();
        assert!(matches!(observed, ObligationRebuildError::Activity(_)));
        assert!(matches!(rebuilt, ObligationRebuildError::Activity(_)));
        assert_eq!(observed.to_string(), rebuilt.to_string());
    }

    #[test]
    fn daily_boundary_candidates_match_recovery_with_paper_anchor() {
        let dir = tempdir().unwrap();
        let source_path = dir.path().join("source.log");
        let paper_path = dir.path().join("paper.log");
        let wallet = WalletAddress::from_hex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        let mut paper_writer = Writer::open(&paper_path).unwrap();
        append_paper_record(&mut paper_writer, &qualification_start(wallet), 100);
        append_paper_record(
            &mut paper_writer,
            &PaperLogRecord::PortfolioMark(Box::new(PortfolioMark {
                boundary_receipt: AppendReceipt {
                    sequence: pe_core_types::EventSeq(7),
                    this_hash: blake3::Hash::from_bytes([7; 32]),
                },
                cutoff_unix: 200,
                source_tail: empty_tail(),
                financial_prefix_seq: None,
                prices: Vec::new(),
                cash: rust_decimal::Decimal::ZERO,
                equity: rust_decimal::Decimal::ZERO,
                invalid: None,
            })),
            201,
        );
        drop(paper_writer);

        let mut source_writer = Writer::open(&source_path).unwrap();
        for cutoff_unix in [300, 150] {
            append_source_frame(
                &mut source_writer,
                DAILY_BOUNDARY_SOURCE_ID,
                &serde_json::to_vec(&serde_json::json!({
                    "kind": "daily_boundary",
                    "cutoff_unix": cutoff_unix,
                }))
                .unwrap(),
                cutoff_unix,
                1,
                1,
            );
        }
        let expected = append_source_frame(
            &mut source_writer,
            DAILY_BOUNDARY_SOURCE_ID,
            &serde_json::to_vec(&serde_json::json!({
                "kind": "daily_boundary",
                "cutoff_unix": 250,
            }))
            .unwrap(),
            250,
            1,
            1,
        );
        drop(source_writer);

        let mut candidates = DailyBoundaryCandidates::default();
        for item in Reader::replay(&source_path).unwrap() {
            candidates.observe_daily_boundary(&item.unwrap().1).unwrap();
        }
        let mut split = ReconciliationObligations::default();
        let anchor = recover_daily_boundary_anchor(
            &paper_path,
            &mut split,
            &PaperStateDb::open(&dir.path().join("paper.sqlite")).unwrap(),
        )
        .unwrap()
        .unwrap();
        recover_daily_boundary_from_candidates(candidates, anchor, &mut split);

        let mut recovered = ReconciliationObligations::default();
        recover_daily_boundary(
            &source_path,
            &paper_path,
            &mut recovered,
            &PaperStateDb::open(&dir.path().join("paper.sqlite")).unwrap(),
        )
        .unwrap();
        assert_eq!(split, recovered);
        assert_eq!(split.boundary_anchor(), Some(200));
        assert_eq!(
            split.pending_boundary(),
            Some(PendingBoundary {
                cutoff_unix: 250,
                receipt: expected,
            })
        );
    }

    #[test]
    fn malformed_daily_boundary_candidates_preserve_errors() {
        let dir = tempdir().unwrap();
        for (name, payload, expected) in [
            (
                "kind",
                serde_json::to_vec(&serde_json::json!({"kind": "other", "cutoff_unix": 1}))
                    .unwrap(),
                "boundary source id carries an unexpected kind",
            ),
            (
                "cutoff",
                serde_json::to_vec(&serde_json::json!({"kind": "daily_boundary"})).unwrap(),
                "boundary cutoff is absent",
            ),
        ] {
            let path = dir.path().join(format!("{name}.log"));
            let mut writer = Writer::open(&path).unwrap();
            append_source_frame(&mut writer, DAILY_BOUNDARY_SOURCE_ID, &payload, 1, 1, 1);
            drop(writer);
            let envelope = Reader::replay(&path).unwrap().next().unwrap().unwrap().1;
            let error = DailyBoundaryCandidates::default()
                .observe_daily_boundary(&envelope)
                .unwrap_err();
            assert!(matches!(
                error,
                ObligationRebuildError::Boundary(ref message) if message == expected
            ));
        }
    }

    #[test]
    fn daily_boundary_recovery_before_financial_start_does_not_read_source_log() {
        let dir = tempdir().unwrap();
        let paper_path = dir.path().join("paper.log");
        let missing_source_path = dir.path().join("source-does-not-exist.log");
        drop(Writer::open(&paper_path).unwrap());

        let mut obligations = ReconciliationObligations::default();
        recover_daily_boundary(
            &missing_source_path,
            &paper_path,
            &mut obligations,
            &PaperStateDb::open(&dir.path().join("paper.sqlite")).unwrap(),
        )
        .unwrap();

        assert!(obligations.boundary_anchor().is_none());
        assert!(obligations.pending_boundary().is_none());
        assert!(!missing_source_path.exists());
    }
}
