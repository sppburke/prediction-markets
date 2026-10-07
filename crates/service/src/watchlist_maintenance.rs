//! Watchlist maintenance tick (#350 WS1 PR-D): inactivity + underperformance knockout with
//! atomic backfill from the Supabase bench.
//!
//! A live wallet is knocked out when EITHER trigger fires (checked each tick):
//!   1. **Inactivity** — idle ≥ `inactivity_threshold_secs`, UNLESS it is a *proven winner*
//!      (≥ `demotion_min_trades` settled fills AND lower-CB edge > 0), which is spared up to
//!      `inactivity_hard_cap_secs`; past the hard cap it is evicted unconditionally.
//!   2. **Underperformance** — [`WalletEdgeStats::should_demote`]: upper-CB edge < 0 AND
//!      trailing-window realized P&L < 0 (`demotion_pnl_window_secs`) AND
//!      ≥ `demotion_min_trades` settled fills.
//!
//! The idle clock is the wallet's activity clock (#511), falling back to its delivery cursor:
//! `ranking_entries.last_trade_unix` seeds it at admission and the [`crate::trade_poller`]
//! advances it, so `idle = now − last observed trade`. There is no admission grace: a wallet whose
//! last trade is already > `inactivity_threshold_secs` ago is eviction-eligible on the next tick.
//! A wallet with neither clock (not yet polled and no seed value) is treated as idle 0: it
//! self-heals to "now" rather than being read as inactive-forever.
//!
//! Freed slots are atomically backfilled via [`LiveWatchlist::replace`] from the top of
//! the batch-pinned survivor bench, excluding the live ∪ evicted sets. The refresh loop and this tick are
//! serialized by a shared [`tokio::sync::Mutex`] writer lock; readers stay lock-free. The
//! realized-edge series both triggers consume comes from the authoritative local `paper_state.db`
//! (`list_fills` + in-process [`ResolutionStore`]), never the best-effort Supabase mirror.
//!
//! ## Membership modes (2026-07-03 run28 cutover)
//!
//! [`MembershipMode`] selects who owns MEMBERSHIP between ranking batches:
//!
//! * [`MembershipMode::Knockout`] (legacy default) — hold-until-knockout: the ranking push
//!   never changes structural membership; knockout+backfill does. A batch transition can
//!   restore a prepared structural wallet that was excluded from live at boot.
//! * [`MembershipMode::FullRerank`] — the ranker owns membership at every batch: on a batch
//!   TRANSITION the newest `latest_ranking` top-`cap` wholesale-REPLACES the live set
//!   ([`apply_full_rerank_swap`]) — wallets re-earn their slot each push (run28 `docs/33` §5:
//!   the knockout-only policy was the worst tested; full re-rank the most robust). Memoryless
//!   by design: the ranker's verdict overrides live demotion memory at each batch (the evicted
//!   set clears), while the knockout above still runs BETWEEN batches as the intra-cycle
//!   safety rail — a readmitted bleeder is re-demotable on the next tick. A failed fetch,
//!   admission preparation, or structural apply leaves the batch marker unadvanced so the swap
//!   retries next tick.
//!
//! ## Admission preparation (#542)
//!
//! Both membership paths publish only wallets the shared [`crate::watchlist_admission`] preparer
//! has validated against durable reconciled history and the monotonic fence set. The publication
//! lock repeats those checks; the causal positions bracket extends the same serialized attempt.
//!
//! Both modes pin transition reads to the batch identifier that triggered them, so the rows
//! applied and the marker committed always name one batch.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use pe_core_types::WalletAddress;
use pe_event_log::AppendReceipt;
use pe_paper_pnl::ResolutionStore;
use pe_paper_state::{FillRow, PaperStateDb, PaperStateError};
use pe_trader_index::{Watchlist, WatchlistEntry};
use rust_decimal::Decimal;
use time::OffsetDateTime;
use tokio::sync::{Mutex, MutexGuard, watch};
use tracing::{error, info, warn};

use crate::demotion_stat::{WalletEdgeStats, wallet_edge_stats};
use crate::live_watchlist::LiveWatchlist;
use crate::paper_api::ParsedKey;
use crate::paper_recovery::{
    KnockoutCausalArtifact, KnockoutFillArtifact, KnockoutSettlementArtifact, MembershipChange,
    MembershipReason, SealedMembershipEvidence,
};
use crate::runtime_config::{
    AppliedWatchlistCapacity, MAX_ACTIVE_WATCHLIST_SIZE, WatchlistCapacityEpoch,
};
use crate::supabase_reader;
use crate::watchlist_admission::AdmissionPreparer;

/// Who owns watchlist MEMBERSHIP between ranking batches. See the module docs; canonical
/// default in `docs/_GLOSSARY.md` (`watchlist_membership_mode`). Boot-frozen (env/TOML) —
/// the maintenance loop is built once at startup, so changing the mode needs a restart.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MembershipMode {
    /// Hold-until-knockout (legacy): structural membership changes only via knockout + backfill.
    #[default]
    Knockout,
    /// The newest ranking batch's top-`cap` replaces the live set on every batch transition.
    FullRerank,
}

impl MembershipMode {
    /// Parse the `watchlist_membership_mode` config string. `None` for an unknown value —
    /// the caller (`main.rs`) fails fast rather than silently defaulting.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "knockout" => Some(Self::Knockout),
            "full_rerank" => Some(Self::FullRerank),
            _ => None,
        }
    }
}

/// Tuning for the maintenance tick. Every field is sourced from [`crate::config::ServiceConfig`]
/// (defaults registered in `docs/_GLOSSARY.md`). The working-set cap is read from the last
/// successfully [`AppliedWatchlistCapacity`] epoch at the start of every tick.
#[derive(Debug, Clone)]
pub struct MaintenanceConfig {
    /// Seconds between ticks. `0` disables the loop entirely (handled by the caller).
    pub interval_secs: u64,
    /// Idle threshold (seconds) past which a non-proven-winner wallet is evicted.
    pub inactivity_threshold_secs: u64,
    /// Hard ceiling (seconds) past which even a proven winner is evicted for inactivity.
    pub inactivity_hard_cap_secs: u64,
    /// Minimum settled fills for the demotion and proven-winner predicates.
    pub demotion_min_trades: usize,
    /// Empirical-Bernstein confidence level α (never `f64`).
    pub demotion_cb_alpha: Decimal,
    /// Trailing window (seconds) for the demotion realized-P&L conjunct
    /// (`WalletEdgeStats::windowed_pnl`).
    pub demotion_pnl_window_secs: u64,
    /// Who owns membership between ranking batches (module docs; run28 cutover).
    pub membership_mode: MembershipMode,
}

/// Fail-closed structural membership errors. Every variant leaves the in-memory generation
/// unchanged; cursor batches are transactional, so a cursor failure is unchanged too.
#[derive(Debug, thiserror::Error)]
pub enum MembershipApplyError {
    /// Network work was planned against an applied epoch that has since been superseded.
    #[error(
        "stale watchlist capacity plan: expected generation {expected_generation} target {expected_target}, applied generation {applied_generation} target {applied_target}"
    )]
    StaleCapacity {
        expected_generation: u64,
        expected_target: usize,
        applied_generation: u64,
        applied_target: usize,
    },
    /// A freshness-filtered/ranked admission unexpectedly lacked its real last-trade cursor.
    #[error("missing last_trade_unix for newly admitted wallet {wallet}")]
    MissingCursor { wallet: WalletAddress },
    /// SQLite rejected the all-or-nothing cursor batch.
    #[error("persist admission cursors: {0}")]
    Cursor(#[from] PaperStateError),
    #[error("newly admitted wallet {wallet} is durably fenced")]
    FencedAdmission { wallet: WalletAddress },
    #[error("newly admitted wallet {wallet} lacks complete reconciled history")]
    IncompleteHistory { wallet: WalletAddress },
    #[error("newly admitted wallet {wallet} lacks a current causal position validation")]
    UnvalidatedPosition { wallet: WalletAddress },
    #[error("membership evidence receipts differ from the writer-locked wallet mutation")]
    EvidenceMutation,
    #[error("publish synchronized membership: {0}")]
    Publication(PublishError),
    #[error("prepared structural wallet set changed before publication")]
    StaleStructure,
    #[error("capacity request was superseded before publication")]
    CapacitySuperseded,
    #[error("membership proof changed for wallet {wallet}")]
    ProofChanged { wallet: WalletAddress },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalletPublishCause {
    FencedAdmission,
    IncompleteHistory,
    UnvalidatedPosition,
    ProofChanged,
}

impl WalletPublishCause {
    pub fn class(self) -> crate::position_seeder::FailureClass {
        use crate::position_seeder::FailureClass;
        match self {
            Self::FencedAdmission | Self::IncompleteHistory => FailureClass::WalletPersistent,
            Self::UnvalidatedPosition | Self::ProofChanged => FailureClass::WalletTransient,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PublishError {
    #[error("wallet {wallet} publication rejected: {cause:?}")]
    Wallet {
        wallet: WalletAddress,
        cause: WalletPublishCause,
    },
    #[error("prepared structural wallet set changed")]
    StaleStructure,
    #[error("applied capacity changed")]
    StaleCapacity,
    #[error("desired capacity request was superseded")]
    CapacitySuperseded,
    #[error("synchronized paper append uncertain: {0}")]
    UncertainAppend(String),
    #[error("membership publication failed: {0}")]
    Shared(String),
}

impl PublishError {
    pub fn class(&self) -> crate::position_seeder::FailureClass {
        use crate::position_seeder::FailureClass;
        match self {
            Self::Wallet { cause, .. } => cause.class(),
            Self::StaleStructure
            | Self::StaleCapacity
            | Self::CapacitySuperseded
            | Self::UncertainAppend(_)
            | Self::Shared(_) => FailureClass::Shared,
        }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Self::Wallet {
                cause: WalletPublishCause::FencedAdmission,
                ..
            } => "publication.fenced",
            Self::Wallet {
                cause: WalletPublishCause::IncompleteHistory,
                ..
            } => "publication.incomplete_history",
            Self::Wallet {
                cause: WalletPublishCause::UnvalidatedPosition,
                ..
            } => "publication.unvalidated_position",
            Self::Wallet {
                cause: WalletPublishCause::ProofChanged,
                ..
            } => "publication.proof_changed",
            Self::StaleStructure => "publication.stale_structure",
            Self::StaleCapacity => "publication.stale_capacity",
            Self::CapacitySuperseded => "publication.capacity_superseded",
            Self::UncertainAppend(_) => "publication.uncertain_append",
            Self::Shared(_) => "publication.shared",
        }
    }
}

impl MembershipApplyError {
    pub fn class(&self) -> crate::position_seeder::FailureClass {
        use crate::position_seeder::FailureClass;
        match self {
            Self::MissingCursor { .. }
            | Self::FencedAdmission { .. }
            | Self::IncompleteHistory { .. } => FailureClass::WalletPersistent,
            Self::UnvalidatedPosition { .. } | Self::ProofChanged { .. } => {
                FailureClass::WalletTransient
            }
            Self::Publication(error) => error.class(),
            Self::StaleCapacity { .. }
            | Self::StaleStructure
            | Self::CapacitySuperseded
            | Self::Cursor(_)
            | Self::EvidenceMutation => FailureClass::Shared,
        }
    }

    pub fn into_publish(self) -> PublishError {
        match self {
            Self::FencedAdmission { wallet } => PublishError::Wallet {
                wallet,
                cause: WalletPublishCause::FencedAdmission,
            },
            Self::IncompleteHistory { wallet } => PublishError::Wallet {
                wallet,
                cause: WalletPublishCause::IncompleteHistory,
            },
            Self::UnvalidatedPosition { wallet } => PublishError::Wallet {
                wallet,
                cause: WalletPublishCause::UnvalidatedPosition,
            },
            Self::ProofChanged { wallet } => PublishError::Wallet {
                wallet,
                cause: WalletPublishCause::ProofChanged,
            },
            Self::StaleStructure => PublishError::StaleStructure,
            Self::StaleCapacity { .. } => PublishError::StaleCapacity,
            Self::CapacitySuperseded => PublishError::CapacitySuperseded,
            Self::Publication(error) => error,
            Self::MissingCursor { .. } | Self::Cursor(_) | Self::EvidenceMutation => {
                PublishError::Shared(self.to_string())
            }
        }
    }

    pub fn deferrable_wallet(&self) -> Option<(WalletAddress, &'static str)> {
        match self {
            Self::MissingCursor { wallet } => Some((*wallet, "seed.missing_cursor")),
            Self::FencedAdmission { wallet } => Some((*wallet, "publication.fenced")),
            Self::IncompleteHistory { wallet } => Some((*wallet, "publication.incomplete_history")),
            Self::UnvalidatedPosition { wallet } => {
                Some((*wallet, "publication.unvalidated_position"))
            }
            Self::ProofChanged { wallet } => Some((*wallet, "publication.proof_changed")),
            Self::Publication(error @ PublishError::Wallet { wallet, .. }) => {
                Some((*wallet, error.kind()))
            }
            Self::Publication(
                PublishError::StaleStructure
                | PublishError::StaleCapacity
                | PublishError::CapacitySuperseded
                | PublishError::UncertainAppend(_)
                | PublishError::Shared(_),
            )
            | Self::StaleCapacity { .. }
            | Self::StaleStructure
            | Self::CapacitySuperseded
            | Self::Cursor(_)
            | Self::EvidenceMutation => None,
        }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Self::Publication(error) => error.kind(),
            Self::StaleCapacity { .. } => "publication.stale_capacity",
            Self::StaleStructure => "publication.stale_structure",
            Self::CapacitySuperseded => "publication.capacity_superseded",
            Self::MissingCursor { .. } => "seed.missing_cursor",
            Self::Cursor(_) => "seed.paper_state",
            Self::FencedAdmission { .. } => "publication.fenced",
            Self::IncompleteHistory { .. } => "publication.incomplete_history",
            Self::UnvalidatedPosition { .. } => "publication.unvalidated_position",
            Self::ProofChanged { .. } => "publication.proof_changed",
            Self::EvidenceMutation => "publication.evidence_mutation",
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct PublicationBinding {
    pub structural: HashSet<WalletAddress>,
    pub digests: Vec<(WalletAddress, String)>,
}

impl PublicationBinding {
    pub(crate) fn capture(
        structural: HashSet<WalletAddress>,
        proofs: &[(
            WalletAddress,
            crate::paper_recovery::MembershipProofManifest,
        )],
    ) -> Result<Self, crate::paper_recovery::MembershipProofError> {
        let mut digests = Vec::with_capacity(proofs.len());
        for (wallet, proof) in proofs {
            digests.push((*wallet, proof.digest()?));
        }
        Ok(Self {
            structural,
            digests,
        })
    }

    #[cfg(feature = "scenario")]
    pub fn for_additions(
        structural: HashSet<WalletAddress>,
        state: &PaperStateDb,
        additions: &[WalletAddress],
    ) -> Result<Self, crate::paper_recovery::MembershipProofError> {
        let mut proofs = Vec::with_capacity(additions.len());
        for wallet in additions {
            proofs.push((
                *wallet,
                crate::paper_recovery::MembershipProofManifest::capture(state, &[*wallet])?,
            ));
        }
        Self::capture(structural, &proofs)
    }
}

/// Durable context for one structural publication. Exact removed/added sets and capacity are
/// derived under the writer lock immediately before the orchestrator handoff.
#[derive(Debug, Clone)]
pub struct MembershipPublication {
    pub reason: MembershipReason,
    pub ranking_batch_id: Option<i64>,
    pub evidence: serde_json::Value,
    pub binding: PublicationBinding,
}

/// Process-local checks carried to the single-owner publication boundary; never serialized.
#[derive(Debug, Default)]
pub struct MembershipCommit {
    pub seeds: Vec<(WalletAddress, i64)>,
    pub capacity: Option<MembershipCapacityCheck>,
    pub reentries: Vec<WalletAddress>,
    pub binding: PublicationBinding,
}

#[derive(Debug)]
pub enum MembershipCapacityCheck {
    Unchanged {
        applied: AppliedWatchlistCapacity,
        expected: WatchlistCapacityEpoch,
    },
    Transition {
        applied: AppliedWatchlistCapacity,
        desired: watch::Receiver<WatchlistCapacityEpoch>,
        request: WatchlistCapacityEpoch,
    },
}

impl MembershipCommit {
    /// Called under the writer lock immediately before appending the membership record.
    pub fn recheck_and_seed(
        &self,
        paper_state: &PaperStateDb,
        live: &LiveWatchlist,
        change: &MembershipChange,
        replacements: &[WatchlistEntry],
    ) -> Result<(), MembershipApplyError> {
        match &self.capacity {
            Some(MembershipCapacityCheck::Unchanged { applied, expected }) => {
                let current = applied.load();
                if current != *expected {
                    return Err(stale_capacity_error(*expected, current));
                }
            }
            Some(MembershipCapacityCheck::Transition {
                desired, request, ..
            }) if *desired.borrow() != *request => {
                return Err(MembershipApplyError::CapacitySuperseded);
            }
            _ => {}
        }
        if live.structural_membership() != self.binding.structural {
            return Err(MembershipApplyError::StaleStructure);
        }
        let added: HashSet<_> = change.added.iter().copied().collect();
        let bound: HashSet<_> = self
            .binding
            .digests
            .iter()
            .map(|(wallet, _)| *wallet)
            .collect();
        if change.added.len() != self.binding.digests.len() || added != bound {
            return Err(MembershipApplyError::EvidenceMutation);
        }
        for (wallet, digest) in &self.binding.digests {
            let current =
                crate::paper_recovery::MembershipProofManifest::capture(paper_state, &[*wallet]);
            match current {
                Ok(current)
                    if current.digest().map_err(|error| {
                        MembershipApplyError::Publication(PublishError::Shared(error.to_string()))
                    })? == *digest => {}
                Ok(_) => return Err(MembershipApplyError::ProofChanged { wallet: *wallet }),
                Err(crate::paper_recovery::MembershipProofError::MissingHistory(_)) => {
                    return Err(MembershipApplyError::IncompleteHistory { wallet: *wallet });
                }
                Err(crate::paper_recovery::MembershipProofError::MissingValidation(_)) => {
                    return Err(MembershipApplyError::UnvalidatedPosition { wallet: *wallet });
                }
                Err(error) if error.class() != crate::position_seeder::FailureClass::Shared => {
                    return Err(MembershipApplyError::ProofChanged { wallet: *wallet });
                }
                Err(error) => {
                    return Err(MembershipApplyError::Publication(PublishError::Shared(
                        error.to_string(),
                    )));
                }
            }
        }
        recheck_admissions(paper_state, &change.added)?;
        let current = live.structural_membership();
        let (removed, added) = match change.reason {
            MembershipReason::FullRerank | MembershipReason::CapacityChange => {
                ranked_membership_change_set(&current, replacements, change.capacity)
            }
            _ => {
                let removed: HashSet<_> = change.removed.iter().copied().collect();
                let added =
                    planned_admission_wallets(&current, &removed, replacements, change.capacity);
                let removed = current.intersection(&removed).copied().collect();
                (removed, added)
            }
        };
        let same_wallets = |left: &[WalletAddress], right: &[WalletAddress]| {
            left.iter().copied().collect::<HashSet<_>>()
                == right.iter().copied().collect::<HashSet<_>>()
        };
        if !same_wallets(&removed, &change.removed) || !same_wallets(&added, &change.added) {
            return Err(MembershipApplyError::EvidenceMutation);
        }
        recheck_publication_evidence(&change.evidence, &change.removed, &change.added)?;
        // #511: insert-only — never jump an existing (possibly HELD) delivery cursor.
        paper_state.seed_cursors_if_absent(&self.seeds)?;
        Ok(())
    }

    /// Commit the capacity epoch with the successful live replacement under the writer lock.
    pub fn commit_capacity(&self) {
        if let Some(MembershipCapacityCheck::Transition {
            applied, request, ..
        }) = &self.capacity
        {
            applied.store(*request);
        }
    }
}

fn remove_loaded_fences(
    live: &LiveWatchlist,
    paper_state: &PaperStateDb,
) -> Result<(), MembershipApplyError> {
    let fenced: HashSet<_> = paper_state
        .wallet_fences()?
        .into_iter()
        .map(|record| record.wallet)
        .collect();
    live.remove_fenced(&fenced);
    Ok(())
}

fn recheck_admissions(
    paper_state: &PaperStateDb,
    admissions: &[WalletAddress],
) -> Result<(), MembershipApplyError> {
    for wallet in admissions {
        if paper_state.is_wallet_fenced(wallet)? {
            return Err(MembershipApplyError::FencedAdmission { wallet: *wallet });
        }
        if !paper_state.wallet_history_complete(wallet)? {
            return Err(MembershipApplyError::IncompleteHistory { wallet: *wallet });
        }
        if !paper_state.position_validation_current(wallet)? {
            return Err(MembershipApplyError::UnvalidatedPosition { wallet: *wallet });
        }
    }
    Ok(())
}

pub(crate) fn planned_live_reentries(
    live: &LiveWatchlist,
    incoming: &[WatchlistEntry],
) -> Vec<WalletAddress> {
    let structural = live.structural_membership();
    let present = live
        .snapshot()
        .entries
        .iter()
        .map(|entry| entry.wallet)
        .collect::<HashSet<_>>();
    // Scan every incoming survivor: in knockout mode a structural wallet can rank below the
    // cap. Candidates are structural, so bounded by capacity; the locked apply bounds live.
    incoming
        .iter()
        .filter(|entry| structural.contains(&entry.wallet) && !present.contains(&entry.wallet))
        .map(|entry| entry.wallet)
        .collect()
}

/// Caller holds the shared writer lock. Each wallet uses the entry in the applied batch;
/// failed or superseded admissions remain outside live membership.
pub(crate) fn apply_live_reentries(
    live: &LiveWatchlist,
    paper_state: &PaperStateDb,
    prepared: &[WalletAddress],
    applied_entries: &[WatchlistEntry],
    cap: usize,
) -> crate::watchlist_admission::AdmissionOutcome {
    let structural = live.structural_membership();
    let mut present = live
        .snapshot()
        .entries
        .iter()
        .map(|entry| entry.wallet)
        .collect::<HashSet<_>>();
    let prepared = prepared.iter().copied().collect::<HashSet<_>>();
    let mut admitted = Vec::new();
    let mut deferred = Vec::new();
    for entry in applied_entries {
        if present.len() >= cap {
            break;
        }
        let wallet = entry.wallet;
        if !prepared.contains(&wallet) || !structural.contains(&wallet) || present.contains(&wallet)
        {
            continue;
        }
        if let Err(error) = recheck_admissions(paper_state, &[wallet]) {
            deferred.push(crate::watchlist_admission::Deferral {
                completed_at: Some(tokio::time::Instant::now()),
                wallet,
                stage: "locked_apply",
                class: error.class(),
                kind: error.kind(),
                message: error.to_string(),
            });
            continue;
        }
        present.insert(wallet);
        admitted.push(entry.clone());
    }
    if !admitted.is_empty() {
        live.replace(&HashSet::new(), &admitted, cap);
    }
    crate::watchlist_admission::AdmissionOutcome {
        started: Vec::new(),
        unstarted: Vec::new(),
        admitted: admitted.into_iter().map(|entry| entry.wallet).collect(),
        deferred,
    }
}

/// Mirror the locked live-only projection step in an external scenario publisher.
#[cfg(feature = "scenario")]
pub fn scenario_apply_live_reentries(
    live: &LiveWatchlist,
    paper_state: &PaperStateDb,
    prepared: &[WalletAddress],
    applied_entries: &[WatchlistEntry],
    cap: usize,
) -> crate::watchlist_admission::AdmissionOutcome {
    apply_live_reentries(live, paper_state, prepared, applied_entries, cap)
}

fn recheck_publication_evidence(
    publication_evidence: &serde_json::Value,
    removed: &[WalletAddress],
    added: &[WalletAddress],
) -> Result<(), MembershipApplyError> {
    // The public helper predates sealed qualification and remains used by non-qualifying
    // scenario callers with documentary JSON. Production publishers always construct this enum;
    // qualification independently rejects an untyped durable record.
    let Ok(evidence) =
        serde_json::from_value::<SealedMembershipEvidence>(publication_evidence.clone())
    else {
        return Ok(());
    };
    if !evidence.matches_wallet_mutation(removed, added) {
        return Err(MembershipApplyError::EvidenceMutation);
    }
    Ok(())
}

fn stale_capacity_error(
    expected: WatchlistCapacityEpoch,
    applied: WatchlistCapacityEpoch,
) -> MembershipApplyError {
    MembershipApplyError::StaleCapacity {
        expected_generation: expected.generation,
        expected_target: expected.target,
        applied_generation: applied.generation,
        applied_target: applied.target,
    }
}

/// Why a live wallet was knocked out (drives the `wallet_lifecycle_events.reason` audit text).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KnockoutReason {
    /// Idle ≥ threshold and not a spared proven winner.
    Inactivity,
    /// Idle ≥ the hard cap (a proven winner is no longer spared).
    InactivityHardCap,
    /// Statistical demotion: upper-CB edge < 0 AND trailing-window realized P&L < 0
    /// AND enough settled trades.
    Underperformance,
    /// Full-re-rank rotation ([`MembershipMode::FullRerank`]): the wallet fell out of the
    /// newest batch's top-`cap`. Audit-only — never returned by [`knockout_decision`].
    RankerRotation,
}

impl KnockoutReason {
    /// Audit text written to `wallet_lifecycle_events.reason`.
    #[must_use]
    pub fn reason_text(self) -> &'static str {
        match self {
            Self::Inactivity => "inactive>72h",
            Self::InactivityHardCap => "inactive>7d (hard cap)",
            Self::Underperformance => "upper_cb_edge<0 & windowed_pnl<0",
            Self::RankerRotation => "full_rerank: dropped from ranking top-N",
        }
    }
}

impl From<KnockoutReason> for crate::paper_recovery::MembershipReason {
    fn from(reason: KnockoutReason) -> Self {
        match reason {
            KnockoutReason::Inactivity => Self::KnockoutInactivity,
            KnockoutReason::InactivityHardCap => Self::KnockoutInactivityHardCap,
            KnockoutReason::Underperformance => Self::KnockoutUnderperformance,
            // A rotation eviction is published as part of its full rerank, never as a knockout.
            KnockoutReason::RankerRotation => Self::FullRerank,
        }
    }
}

/// A decided eviction, carrying the audit fields for its lifecycle row.
#[derive(Debug, Clone)]
pub struct Eviction {
    /// The wallet to remove from the live set.
    pub wallet: WalletAddress,
    /// Which trigger fired.
    pub reason: KnockoutReason,
    /// Lifetime realized P&L (dollars) if the wallet had settled fills; `None` when no
    /// stats exist. Audit field — the demotion *decision* uses the trailing-window sum.
    pub live_pnl: Option<Decimal>,
    /// Settled-fill count observed for the wallet (`0` when no stats exist).
    pub trades_observed: usize,
    /// The wallet's inactivity clock at eviction: activity clock (#511), delivery-cursor fallback;
    /// `None` for a never-polled wallet evicted on a non-inactivity trigger.
    pub last_trade_unix: Option<i64>,
}

/// Decide whether a single live wallet is knocked out this tick. Pure.
///
/// Underperformance takes reason-precedence over inactivity: it is the more specific, actionable
/// signal and its trailing-window realized-P&L AND-gate is the safety net for the CB constants.
///
/// # Precondition
/// `last_ts` is the wallet's inactivity clock: activity clock (#511), delivery-cursor fallback.
/// `None` means the wallet has not yet been polled and is treated as just-admitted (idle 0) —
/// never inactive-evicted this tick.
#[must_use]
pub fn knockout_decision(
    last_ts: Option<i64>,
    stats: Option<&WalletEdgeStats>,
    cfg: &MaintenanceConfig,
    now_unix: i64,
) -> Option<KnockoutReason> {
    if let Some(s) = stats
        && s.should_demote(cfg.demotion_min_trades)
    {
        return Some(KnockoutReason::Underperformance);
    }

    let idle = match last_ts {
        Some(ts) => now_unix.saturating_sub(ts),
        None => 0, // not-yet-polled wallet self-heals to "now"
    };
    let threshold = i64::try_from(cfg.inactivity_threshold_secs).unwrap_or(i64::MAX);
    let hard_cap = i64::try_from(cfg.inactivity_hard_cap_secs).unwrap_or(i64::MAX);
    if idle >= threshold {
        let proven = stats.is_some_and(|s| s.is_proven_winner(cfg.demotion_min_trades));
        if !proven {
            return Some(KnockoutReason::Inactivity);
        }
        if idle >= hard_cap {
            return Some(KnockoutReason::InactivityHardCap);
        }
    }
    None
}

/// Decide all evictions for the current live set. Pure: no I/O.
///
/// `cursors` maps each live wallet to its inactivity clock (activity clock, delivery-cursor
/// fallback; `None` = not yet polled). `stats` is keyed
/// by leader hex (`WalletAddress::to_string`, canonical lowercase `0x…`) per [`wallet_edge_stats`].
#[must_use]
pub fn decide_evictions(
    live: &Watchlist,
    stats: &HashMap<String, WalletEdgeStats>,
    cursors: &HashMap<WalletAddress, Option<i64>>,
    cfg: &MaintenanceConfig,
    now_unix: i64,
) -> Vec<Eviction> {
    live.entries
        .iter()
        .filter_map(|e| {
            let wallet = e.wallet;
            let s = stats.get(&wallet.to_string());
            let last_ts = cursors.get(&wallet).copied().flatten();
            knockout_decision(last_ts, s, cfg, now_unix).map(|reason| Eviction {
                wallet,
                reason,
                live_pnl: s.map(|st| st.realized_pnl),
                trades_observed: s.map_or(0, |st| st.settled_count),
                last_trade_unix: last_ts,
            })
        })
        .collect()
}

fn knockout_causal_input(
    eviction: &Eviction,
    cfg: &MaintenanceConfig,
    evaluated_at_unix: i64,
    fills: &[FillRow],
    resolutions: &ResolutionStore,
) -> Option<(MembershipReason, KnockoutCausalArtifact)> {
    if matches!(
        eviction.reason,
        KnockoutReason::Inactivity | KnockoutReason::InactivityHardCap
    ) && eviction.last_trade_unix.is_none()
    {
        return None;
    }
    let wallet_hex = eviction.wallet.to_string();
    let mut retained_fills = Vec::new();
    let mut retained_settlements = Vec::new();
    let mut settled_markets = HashSet::new();
    for fill in fills {
        if ParsedKey::from_key(&fill.idempotency_key).leader.as_deref() != Some(&wallet_hex) {
            continue;
        }
        let Some(settlement) = resolutions.settlement_info(&fill.market_id) else {
            continue;
        };
        retained_fills.push(KnockoutFillArtifact::from_row(fill));
        if settled_markets.insert(fill.market_id.clone()) {
            retained_settlements.push(KnockoutSettlementArtifact::from_info(
                fill.market_id.clone(),
                settlement,
            ));
        }
    }
    let reason = MembershipReason::from(eviction.reason);
    Some((
        reason,
        KnockoutCausalArtifact {
            wallet: eviction.wallet,
            evaluated_at_unix,
            last_trade_unix: eviction.last_trade_unix,
            inactivity_threshold_secs: cfg.inactivity_threshold_secs,
            inactivity_hard_cap_secs: cfg.inactivity_hard_cap_secs,
            demotion_min_trades: cfg.demotion_min_trades,
            demotion_cb_alpha: cfg.demotion_cb_alpha,
            demotion_pnl_window_secs: cfg.demotion_pnl_window_secs,
            fills: retained_fills,
            settlements: retained_settlements,
        },
    ))
}

/// Pure wallet-identity owner for knockout/backfill admission selection. Qualification reuses
/// this function against receipt-bound candidate rows.
pub(crate) fn planned_admission_wallets(
    current: &HashSet<WalletAddress>,
    removed: &HashSet<WalletAddress>,
    candidates: &[WatchlistEntry],
    cap: usize,
) -> Vec<WalletAddress> {
    let mut present: HashSet<WalletAddress> = current
        .iter()
        .filter(|wallet| !removed.contains(wallet))
        .copied()
        .collect();
    let mut size = present.len().min(cap);
    let mut admitted = Vec::new();
    for candidate in candidates {
        if size >= cap {
            break;
        }
        if removed.contains(&candidate.wallet) || !present.insert(candidate.wallet) {
            continue;
        }
        admitted.push(candidate.wallet);
        size += 1;
    }
    admitted
}

/// Re-run the ranked-set membership owner using only structural wallet identity.
/// The exact membership change an incoming ranked set produces against `current`: the wallets
/// dropped because they fall outside the incoming top-`cap`, and the wallets newly admitted.
///
/// This is the single owner of that computation (#542): the structural apply publishes exactly
/// this admission set under the writer lock, and the preparer installs exactly this set before
/// the lock is taken, so the two can never disagree. Duplicate wallets and an incoming slice
/// longer than `cap` (neither is produced by the `limit`-bounded ranking reads) resolve the same
/// way on both sides because [`planned_admission_wallets`] and [`LiveWatchlist::replace`] share the
/// same walk.
#[cfg(test)]
pub(crate) fn ranked_membership_change(
    current: &[WatchlistEntry],
    incoming: &[WatchlistEntry],
    cap: usize,
) -> (Vec<WalletAddress>, Vec<WalletAddress>) {
    let current_wallets: Vec<WalletAddress> = current.iter().map(|entry| entry.wallet).collect();
    ranked_membership_change_wallets(&current_wallets, incoming, cap)
}

pub(crate) fn ranked_membership_change_set(
    current: &HashSet<WalletAddress>,
    incoming: &[WatchlistEntry],
    cap: usize,
) -> (Vec<WalletAddress>, Vec<WalletAddress>) {
    let mut wallets = current.iter().copied().collect::<Vec<_>>();
    wallets.sort_unstable_by_key(|wallet| wallet.0);
    ranked_membership_change_wallets(&wallets, incoming, cap)
}

/// Wallet-identity core of [`ranked_membership_change`]: the runtime passes its live entries,
/// the offline verifier passes the membership it replayed from the paper log. One rule, two
/// callers, so the sealed `MembershipChanged` mutation can never disagree with what production
/// published for the same ranking artifact.
pub(crate) fn ranked_membership_change_wallets(
    current: &[WalletAddress],
    incoming: &[WatchlistEntry],
    cap: usize,
) -> (Vec<WalletAddress>, Vec<WalletAddress>) {
    let incoming_set: HashSet<WalletAddress> = incoming
        .iter()
        .take(cap)
        .map(|entry| entry.wallet)
        .collect();
    let dropped: Vec<WalletAddress> = current
        .iter()
        .copied()
        .filter(|wallet| !incoming_set.contains(wallet))
        .collect();
    let removed: HashSet<WalletAddress> = dropped.iter().copied().collect();
    let current_set: HashSet<WalletAddress> = current.iter().copied().collect();
    let admissions = planned_admission_wallets(&current_set, &removed, incoming, cap);
    (dropped, admissions)
}

pub(crate) fn admission_seeds(
    admissions: &[WalletAddress],
    last_trade: &HashMap<WalletAddress, i64>,
) -> Result<Vec<(WalletAddress, i64)>, MembershipApplyError> {
    admissions
        .iter()
        .map(|wallet| {
            last_trade
                .get(wallet)
                .copied()
                .map(|timestamp| (*wallet, timestamp))
                .ok_or(MembershipApplyError::MissingCursor { wallet: *wallet })
        })
        .collect()
}

pub(crate) struct PlannedMembership {
    pub entries: Vec<WatchlistEntry>,
    pub last_trade: HashMap<WalletAddress, i64>,
    pub additions: Vec<WalletAddress>,
    pub proofs: Vec<(
        WalletAddress,
        crate::paper_recovery::MembershipProofManifest,
    )>,
    pub binding: PublicationBinding,
    pub deferrals: Vec<crate::watchlist_admission::Deferral>,
}

pub(crate) struct PlanningAbort {
    pub kind: &'static str,
    pub message: String,
    pub deferrals: Vec<crate::watchlist_admission::Deferral>,
}

async fn audit_knockout_abort(
    preparer: &AdmissionPreparer,
    batch_id: Option<i64>,
    deferrals: Vec<crate::watchlist_admission::Deferral>,
    kind: &'static str,
) {
    if let Some(batch_id) = batch_id {
        preparer
            .record_deferrals(
                crate::watchlist_admission::DeferralContext::Knockout { batch_id },
                deferrals,
                crate::watchlist_admission::DeferralOutcome::AbortedShared { kind },
            )
            .await;
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn plan_membership(
    live: &LiveWatchlist,
    preparer: &AdmissionPreparer,
    candidates: &Watchlist,
    last_trade: &HashMap<WalletAddress, i64>,
    mut excluded: HashSet<WalletAddress>,
    selection_cap: usize,
    membership_cap: usize,
    removed: Option<&HashSet<WalletAddress>>,
    prepared: &mut HashSet<WalletAddress>,
    deadline: Option<tokio::time::Instant>,
    mut sync: Option<&mut BatchSync>,
) -> Result<PlannedMembership, PlanningAbort> {
    let mut deferrals = Vec::new();
    for _ in 0..=candidates.entries.len() {
        let (selected, selected_last_trade) = supabase_reader::select_membership(
            candidates.clone(),
            last_trade.clone(),
            &excluded,
            selection_cap,
        );
        let structural = live.structural_membership();
        let additions = match removed {
            Some(removed) => {
                planned_admission_wallets(&structural, removed, &selected.entries, membership_cap)
            }
            None => ranked_membership_change_set(&structural, &selected.entries, membership_cap).1,
        };
        let new = additions
            .iter()
            .filter(|wallet| !prepared.contains(*wallet))
            .copied()
            .collect::<Vec<_>>();
        let mut changed = false;
        for wallet in &additions {
            if !selected_last_trade.contains_key(wallet) {
                changed |= excluded.insert(*wallet);
                deferrals.push(crate::watchlist_admission::Deferral {
                    completed_at: Some(tokio::time::Instant::now()),
                    wallet: *wallet,
                    stage: "seed",
                    class: crate::position_seeder::FailureClass::WalletPersistent,
                    kind: "seed.missing_cursor",
                    message: format!("missing last_trade_unix for {wallet}"),
                });
            }
        }
        if changed {
            if let Some(sync) = sync.as_deref_mut() {
                park_persistent(sync, preparer.paper_state(), &deferrals);
            }
            continue;
        }
        match preparer
            .prepare_ranked_until(
                &new,
                &selected_last_trade,
                deadline,
                sync.as_ref().map_or(
                    crate::watchlist_admission::AdmissionContext::Capacity,
                    |sync| crate::watchlist_admission::AdmissionContext::Addition {
                        first: !sync.reentries_first,
                    },
                ),
            )
            .await
        {
            Ok(outcome) => {
                if let Some(sync) = sync.as_deref_mut() {
                    sync.completed(
                        preparer.paper_state(),
                        &outcome.started,
                        &outcome.admitted,
                        &outcome.deferred,
                        &outcome.unstarted,
                    );
                }
                for wallet in outcome.unstarted {
                    changed |= excluded.insert(wallet);
                }
                prepared.extend(outcome.admitted);
                for deferral in outcome.deferred {
                    changed |= excluded.insert(deferral.wallet);
                    deferrals.push(deferral);
                }
            }
            Err(abort) => {
                if let Some(sync) = sync.as_deref_mut() {
                    sync.completed(
                        preparer.paper_state(),
                        &abort.started,
                        &abort.admitted,
                        &abort.deferred,
                        &abort.unstarted,
                    );
                }
                if let Some(sync) = sync.as_deref_mut() {
                    park_persistent(sync, preparer.paper_state(), &abort.deferred);
                }
                deferrals.extend(abort.deferred);
                return Err(PlanningAbort {
                    kind: abort.cause.kind(),
                    message: abort.cause.to_string(),
                    deferrals,
                });
            }
        }
        if changed {
            if let Some(sync) = sync.as_deref_mut() {
                park_persistent(sync, preparer.paper_state(), &deferrals);
            }
            continue;
        }
        let (proofs, proof_deferrals) = match preparer.capture_proofs(&additions) {
            Ok(result) => result,
            Err(abort) => {
                if let Some(sync) = sync.as_deref_mut() {
                    park_persistent(sync, preparer.paper_state(), &abort.deferred);
                }
                deferrals.extend(abort.deferred);
                return Err(PlanningAbort {
                    kind: abort.cause.kind(),
                    message: abort.cause.to_string(),
                    deferrals,
                });
            }
        };
        for deferral in proof_deferrals {
            changed |= excluded.insert(deferral.wallet);
            deferrals.push(deferral);
        }
        if changed {
            if let Some(sync) = sync.as_deref_mut() {
                park_persistent(sync, preparer.paper_state(), &deferrals);
            }
            continue;
        }
        let binding =
            PublicationBinding::capture(structural, &proofs).map_err(|error| PlanningAbort {
                kind: "proof.digest",
                message: error.to_string(),
                deferrals: std::mem::take(&mut deferrals),
            })?;
        return Ok(PlannedMembership {
            entries: selected.entries,
            last_trade: selected_last_trade,
            additions,
            proofs,
            binding,
            deferrals,
        });
    }
    Err(PlanningAbort {
        kind: "planning.bound",
        message: "wallet exclusion loop exceeded candidate bound".to_owned(),
        deferrals,
    })
}

/// Apply the decided evictions and backfill freed slots atomically under the writer lock.
///
/// The exact admission set is computed from the locked generation, all real last-trade cursors
/// are committed in one SQLite transaction, and only then is membership published. An epoch
/// mismatch, missing timestamp, or SQLite failure leaves membership unchanged.
#[allow(clippy::too_many_arguments)]
pub async fn apply_evictions_and_backfill(
    live: &LiveWatchlist,
    paper_state: &PaperStateDb,
    writer_lock: &Mutex<()>,
    publisher: &AdmissionPreparer,
    publication: MembershipPublication,
    applied_capacity: &AppliedWatchlistCapacity,
    expected_capacity: WatchlistCapacityEpoch,
    removed: &HashSet<WalletAddress>,
    candidates: &[WatchlistEntry],
    candidate_last_trade: &HashMap<WalletAddress, i64>,
) -> Result<(usize, Option<AppendReceipt>), MembershipApplyError> {
    let _guard = writer_lock.lock().await;
    let applied = applied_capacity.load();
    if applied != expected_capacity {
        return Err(stale_capacity_error(expected_capacity, applied));
    }
    remove_loaded_fences(live, paper_state)?;
    let current = live.structural_membership();
    let admissions =
        planned_admission_wallets(&current, removed, candidates, expected_capacity.target);
    let mut actual_removed = removed
        .iter()
        .filter(|wallet| current.contains(wallet))
        .copied()
        .collect::<Vec<_>>();
    actual_removed.sort_unstable_by_key(|wallet| wallet.0);
    if actual_removed.is_empty() && admissions.is_empty() {
        return Ok((live.snapshot().entries.len(), None));
    }
    let seeds = admission_seeds(&admissions, candidate_last_trade)?;
    drop(_guard);
    let receipt = publisher
        .publish_membership(
            MembershipChange {
                reason: publication.reason,
                removed: actual_removed,
                added: admissions,
                capacity: expected_capacity.target,
                ranking_batch_id: publication.ranking_batch_id,
                evidence: publication.evidence,
            },
            candidates.to_vec(),
            MembershipCommit {
                seeds,
                capacity: Some(MembershipCapacityCheck::Unchanged {
                    applied: applied_capacity.clone(),
                    expected: expected_capacity,
                }),
                reentries: Vec::new(),
                binding: publication.binding,
            },
        )
        .await
        .map_err(MembershipApplyError::Publication)?;
    Ok((live.snapshot().entries.len(), Some(receipt)))
}

/// Compute an exact ranked membership under the caller's structural-writer mutex, then release
/// it before awaiting the orchestrator's final recheck, cursor persistence and publication.
///
/// Used by both same-cap full reranks and runtime capacity transitions. Cursor persistence is a
/// fail-closed prerequisite to ArcSwap publication.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn apply_ranked_membership_locked(
    live: &LiveWatchlist,
    paper_state: &PaperStateDb,
    publisher: &AdmissionPreparer,
    publication: MembershipPublication,
    incoming: &[WatchlistEntry],
    incoming_last_trade: &HashMap<WalletAddress, i64>,
    cap: usize,
    writer_guard: MutexGuard<'_, ()>,
    capacity: Option<MembershipCapacityCheck>,
    reentries: &[WalletAddress],
) -> Result<(usize, Vec<WalletAddress>, Option<AppendReceipt>), MembershipApplyError> {
    remove_loaded_fences(live, paper_state)?;
    let current = live.structural_membership();
    let (dropped, admissions) = ranked_membership_change_set(&current, incoming, cap);
    if dropped.is_empty()
        && admissions.is_empty()
        && publication.reason != MembershipReason::CapacityChange
    {
        apply_live_reentries(live, paper_state, reentries, incoming, cap);
        return Ok((live.snapshot().entries.len(), dropped, None));
    }
    let seeds = admission_seeds(&admissions, incoming_last_trade)?;
    drop(writer_guard);
    let receipt = publisher
        .publish_membership(
            MembershipChange {
                reason: publication.reason,
                removed: dropped.clone(),
                added: admissions,
                capacity: cap,
                ranking_batch_id: publication.ranking_batch_id,
                evidence: publication.evidence,
            },
            incoming.to_vec(),
            MembershipCommit {
                seeds,
                capacity,
                reentries: reentries.to_vec(),
                binding: publication.binding,
            },
        )
        .await
        .map_err(MembershipApplyError::Publication)?;
    let total = live.snapshot().entries.len();
    Ok((total, dropped, Some(receipt)))
}

/// Wholesale membership rotation for [`MembershipMode::FullRerank`]. The operation is rejected
/// if network work was planned against an applied capacity epoch that is no longer current.
#[allow(clippy::too_many_arguments)]
pub async fn apply_full_rerank_swap(
    live: &LiveWatchlist,
    paper_state: &PaperStateDb,
    writer_lock: &Mutex<()>,
    publisher: &AdmissionPreparer,
    publication: MembershipPublication,
    applied_capacity: &AppliedWatchlistCapacity,
    expected_capacity: WatchlistCapacityEpoch,
    incoming: &[WatchlistEntry],
    incoming_last_trade: &HashMap<WalletAddress, i64>,
    reentries: &[WalletAddress],
) -> Result<(usize, Vec<WalletAddress>, Option<AppendReceipt>), MembershipApplyError> {
    let _guard = writer_lock.lock().await;
    let applied = applied_capacity.load();
    if applied != expected_capacity {
        return Err(stale_capacity_error(expected_capacity, applied));
    }
    apply_ranked_membership_locked(
        live,
        paper_state,
        publisher,
        publication,
        incoming,
        incoming_last_trade,
        expected_capacity.target,
        _guard,
        Some(MembershipCapacityCheck::Unchanged {
            applied: applied_capacity.clone(),
            expected: expected_capacity,
        }),
        reentries,
    )
    .await
}

/// Cross-tick ranking-batch memory.
///
/// `marker` is the batch whose rows were last applied (or, in knockout mode, last observed).
/// `capacity_generation` is the capacity epoch full-rerank membership was last synced under: a
/// capacity transition publishes rows from its own `latest_ranking` read, which can predate the
/// batch this loop last applied, so the next full-rerank tick re-applies the newest batch even
/// when the marker already names it (#542). The marker itself is never erased — it still decides
/// whether a tick is a genuine batch transition, which is what clears the eviction memory.
pub(crate) struct BatchSync {
    reentries_first: bool,
    attempted_batch_id: Option<i64>,
    cooldowns: HashMap<WalletAddress, tokio::time::Instant>,
    parking_batch: Option<i64>,
    started: usize,
    accepted: usize,
    deferred: usize,
    unstarted: usize,
    marker: Option<i64>,
    capacity_generation: u64,
    knockout_deferred: HashSet<WalletAddress>,
}

fn park_persistent(
    sync: &mut BatchSync,
    paper: &PaperStateDb,
    deferrals: &[crate::watchlist_admission::Deferral],
) {
    for deferral in deferrals {
        if deferral.class == crate::position_seeder::FailureClass::WalletTransient {
            let terminal = deferral
                .completed_at
                .unwrap_or_else(tokio::time::Instant::now);
            sync.cooldowns.insert(
                deferral.wallet,
                terminal + Duration::from_secs(crate::watchlist_admission::ADMISSION_RETRY_SECS),
            );
        }
    }
    sync.knockout_deferred.extend(
        deferrals
            .iter()
            .filter(|deferral| {
                deferral.class == crate::position_seeder::FailureClass::WalletPersistent
                    && !crate::position_seeder::recoverable_fence_failure(
                        paper,
                        &deferral.wallet,
                        deferral.kind,
                    )
            })
            .map(|deferral| deferral.wallet),
    );
}

impl BatchSync {
    fn cooling(&self, wallet: &WalletAddress) -> bool {
        self.cooldowns
            .get(wallet)
            .is_some_and(|end| tokio::time::Instant::now() < *end)
    }

    fn completed(
        &mut self,
        paper: &PaperStateDb,
        started: &[WalletAddress],
        admitted: &[WalletAddress],
        deferred: &[crate::watchlist_admission::Deferral],
        unstarted: &[WalletAddress],
    ) {
        self.started += started.len();
        self.accepted += admitted.len();
        self.deferred += deferred.len();
        self.unstarted += unstarted.len();
        park_persistent(self, paper, deferred);
        for wallet in admitted {
            self.cooldowns.remove(wallet);
        }
    }
}

struct LiveReentryReport {
    before_live: usize,
    after_live: usize,
    admitted: Vec<WalletAddress>,
    deferred: Vec<crate::watchlist_admission::Deferral>,
}

/// What live re-entry does with one live-absent structural member this tick.
#[derive(Debug, PartialEq, Eq)]
enum Reentry {
    /// Prepare it for admission.
    Admit,
    /// Missing or future ranking timestamp: today's seed deferral, parked until another batch.
    Park,
    /// Not eligible on the evidence available now; check again next tick without parking.
    Retry,
}

/// Re-entry eligibility of a live-absent structural member. The pinned batch's
/// `last_trade_unix` admits it inside [`supabase_reader::ACTIVE_WINDOW_HOURS`]. Past that window
/// its own activity clock (#511) admits it when the knockout would keep it, proven winners
/// included, so a restart cannot strand a wallet the live set would have kept. A missing or
/// future batch value parks: neither changes within a batch, and admission seeding would raise
/// the activity clock to a future value.
fn reentry(
    ranked: Option<i64>,
    observed: Option<i64>,
    stats: Option<&HashMap<String, WalletEdgeStats>>,
    wallet: &WalletAddress,
    cfg: &MaintenanceConfig,
    now_unix: i64,
) -> Reentry {
    let Some(ranked) = ranked.filter(|at| *at <= now_unix) else {
        return Reentry::Park;
    };
    if ranked >= now_unix.saturating_sub(supabase_reader::ACTIVE_WINDOW_HOURS * 3_600) {
        return Reentry::Admit;
    }
    let kept = observed.is_some_and(|at| at <= now_unix)
        && stats.is_some_and(|stats| {
            knockout_decision(observed, stats.get(&wallet.to_string()), cfg, now_unix).is_none()
        });
    if kept { Reentry::Admit } else { Reentry::Retry }
}

#[allow(clippy::too_many_arguments)]
async fn live_reentry_tick(
    live: &LiveWatchlist,
    paper_state: &Arc<PaperStateDb>,
    cfg: &MaintenanceConfig,
    client: &reqwest::Client,
    base_url: &str,
    anon_key: &str,
    secret_key: &str,
    writer_lock: &Mutex<()>,
    applied_capacity: &AppliedWatchlistCapacity,
    capacity_epoch: WatchlistCapacityEpoch,
    preparer: &AdmissionPreparer,
    sync: &mut BatchSync,
    held: Option<&(i64, Watchlist, HashMap<WalletAddress, i64>)>,
    attempted: &mut HashSet<WalletAddress>,
    now_unix: i64,
    deadline: Option<tokio::time::Instant>,
) -> Option<LiveReentryReport> {
    let batch_id = sync.marker?;
    // Read the applied batch only when a structural member is live-absent and retryable this
    // tick; an ordinary tick with the whole structural set live makes no ranking read.
    let present = live
        .snapshot()
        .entries
        .iter()
        .map(|entry| entry.wallet)
        .collect::<HashSet<_>>();
    if !live.structural_membership().iter().any(|wallet| {
        !present.contains(wallet)
            && !sync.knockout_deferred.contains(wallet)
            && !attempted.contains(wallet)
            && !sync.cooling(wallet)
    }) {
        return None;
    }
    let fetched;
    let (entries, last_trade) = if let Some((held_id, watchlist, last_trade)) = held
        && *held_id == batch_id
    {
        (&watchlist.entries, last_trade)
    } else {
        fetched = match supabase_reader::fetch_batch(
            client,
            base_url,
            anon_key,
            secret_key,
            batch_id,
            MAX_ACTIVE_WATCHLIST_SIZE,
        )
        .await
        {
            Ok(fetched) => fetched,
            Err(error) => {
                warn!(batch_id, %error, "live reentry: pinned batch fetch failed; retrying next tick");
                return None;
            }
        };
        (&fetched.0.entries, &fetched.1)
    };
    let before_live = live.snapshot().entries.len();
    let mut deferred = Vec::new();
    let retryable = planned_live_reentries(live, entries)
        .into_iter()
        .filter(|wallet| !sync.knockout_deferred.contains(wallet))
        .filter(|wallet| !attempted.contains(wallet) && !sync.cooling(wallet))
        .map(|wallet| (wallet, paper_state.activity(&wallet).unwrap_or(None)))
        .collect::<Vec<_>>();
    // Statistics decide only a stale-ranked wallet with a usable clock: load them once, then.
    let stats = retryable
        .iter()
        .any(|(wallet, observed)| {
            observed.is_some_and(|at| at <= now_unix)
                && reentry(
                    last_trade.get(wallet).copied(),
                    *observed,
                    None,
                    wallet,
                    cfg,
                    now_unix,
                ) == Reentry::Retry
        })
        .then(|| load_edge_stats(paper_state, cfg, now_unix))
        .flatten();
    let candidates = retryable
        .into_iter()
        .filter(|(wallet, observed)| {
            let ranked = last_trade.get(wallet).copied();
            let by_wallet = stats.as_ref().map(|loaded| &loaded.by_wallet);
            match reentry(ranked, *observed, by_wallet, wallet, cfg, now_unix) {
                Reentry::Admit => true,
                Reentry::Retry => false,
                Reentry::Park => {
                    deferred.push(crate::watchlist_admission::Deferral {
                        completed_at: Some(tokio::time::Instant::now()),
                        wallet: *wallet,
                        stage: "seed",
                        class: crate::position_seeder::FailureClass::WalletPersistent,
                        kind: if ranked.is_none() {
                            "seed.missing_cursor"
                        } else {
                            "seed.stale_cursor"
                        },
                        message: format!("missing or future last_trade_unix for {wallet}"),
                    });
                    false
                }
            }
        })
        .map(|(wallet, _)| wallet)
        .collect::<Vec<_>>();
    if candidates.is_empty() && deferred.is_empty() {
        return None;
    }
    park_persistent(sync, paper_state, &deferred);
    let prepared = match preparer
        .prepare_ranked_until(
            &candidates,
            last_trade,
            deadline,
            crate::watchlist_admission::AdmissionContext::Reentry {
                first: sync.reentries_first,
            },
        )
        .await
    {
        Ok(prepared) => prepared,
        Err(abort) => {
            attempted.extend(abort.started.iter().copied());
            sync.completed(
                paper_state,
                &abort.started,
                &abort.admitted,
                &abort.deferred,
                &abort.unstarted,
            );
            let error = abort.cause;
            warn!(batch_id, kind = error.kind(), %error, "live reentry: shared preparation failed; retrying next tick");
            return None;
        }
    };
    attempted.extend(prepared.started.iter().copied());
    sync.completed(
        paper_state,
        &prepared.started,
        &prepared.admitted,
        &prepared.deferred,
        &prepared.unstarted,
    );
    deferred.extend(prepared.deferred);
    let _writer = writer_lock.lock().await;
    if applied_capacity.load() != capacity_epoch {
        warn!(
            batch_id,
            "live reentry: capacity changed before locked apply; retrying next tick"
        );
        return None;
    }
    let applied = apply_live_reentries(
        live,
        paper_state,
        &prepared.admitted,
        entries,
        capacity_epoch.target,
    );
    park_persistent(sync, paper_state, &applied.deferred);
    deferred.extend(applied.deferred);
    let report = LiveReentryReport {
        before_live,
        after_live: live.snapshot().entries.len(),
        admitted: applied.admitted,
        deferred,
    };
    Some(report)
}

async fn record_live_reentry(
    preparer: &AdmissionPreparer,
    sync: &BatchSync,
    mode: MembershipMode,
    report: Option<LiveReentryReport>,
) {
    let Some(report) = report else { return };
    info!(
        before_live = report.before_live,
        after_live = report.after_live,
        admitted = report.admitted.len(),
        deferred = report.deferred.len(),
        "live reentry tick completed"
    );
    if let Some(batch_id) = sync.marker {
        let context = match mode {
            MembershipMode::Knockout => {
                crate::watchlist_admission::DeferralContext::Knockout { batch_id }
            }
            MembershipMode::FullRerank => {
                crate::watchlist_admission::DeferralContext::FullRerank { batch_id }
            }
        };
        preparer
            .record_deferrals(
                context,
                report.deferred,
                crate::watchlist_admission::DeferralOutcome::NoChange,
            )
            .await;
    }
}

/// Stateful, fixed-clock entry point for composed maintenance scenarios.
#[cfg(feature = "scenario")]
pub struct ScenarioMaintenanceState {
    sync: BatchSync,
    evicted: HashSet<WalletAddress>,
}
#[cfg(feature = "scenario")]
impl ScenarioMaintenanceState {
    pub fn new(marker: Option<i64>, capacity_generation: u64) -> Self {
        Self {
            sync: BatchSync {
                marker,
                capacity_generation,
                parking_batch: marker,
                reentries_first: true,
                attempted_batch_id: None,
                knockout_deferred: HashSet::new(),
                cooldowns: HashMap::new(),
                started: 0,
                accepted: 0,
                deferred: 0,
                unstarted: 0,
            },
            evicted: HashSet::new(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn tick(
        &mut self,
        live: &LiveWatchlist,
        paper: &Arc<PaperStateDb>,
        client: &reqwest::Client,
        base_url: &str,
        writer_lock: &Mutex<()>,
        applied: &AppliedWatchlistCapacity,
        preparer: &AdmissionPreparer,
        cfg: &MaintenanceConfig,
        now_unix: i64,
    ) {
        maintenance_tick(
            live,
            paper,
            client,
            base_url,
            "fixture",
            "fixture",
            writer_lock,
            applied,
            preparer,
            cfg,
            applied.load(),
            &mut self.evicted,
            &mut self.sync,
            now_unix,
        )
        .await;
    }
}

/// Run the maintenance tick loop until the process exits.
///
/// `cfg.interval_secs == 0` disables the loop. The first tick fires at startup.
#[allow(clippy::too_many_arguments)]
pub async fn run_maintenance_loop(
    live: LiveWatchlist,
    paper_state: Arc<PaperStateDb>,
    client: reqwest::Client,
    base_url: String,
    anon_key: String,
    secret_key: String,
    writer_lock: Arc<Mutex<()>>,
    cfg: MaintenanceConfig,
    applied_capacity: AppliedWatchlistCapacity,
    preparer: AdmissionPreparer,
    initial_batch_marker: Option<i64>,
    boot_persistent_deferred: HashSet<WalletAddress>,
    boot_cooldowns: HashMap<WalletAddress, tokio::time::Instant>,
) {
    if cfg.interval_secs == 0 {
        info!("watchlist maintenance disabled (maintenance_interval_secs = 0)");
        return;
    }
    if secret_key.is_empty() {
        warn!(
            "watchlist maintenance: no supabase secret key — bench fetch and lifecycle writes may \
             be rejected by RLS"
        );
    }
    let interval = Duration::from_secs(cfg.interval_secs);
    // Wallets evicted under the current ranking batch: excluded from backfill so a just-evicted
    // wallet is not instantly re-admitted with a reset clock. Cleared when a new batch is pushed.
    let mut evicted: HashSet<WalletAddress> = HashSet::new();
    // Financial boot supplies the last ranking batch named by a structurally applied durable
    // generation. Pre-Start boot supplies the batch observed before its moving ranking read.
    // Either way, a newer publication remains a transition for the first tick. A `None` marker
    // (the pre-Start batch read failed) is an ordinary transition too (#542): the first tick
    // applies the batch it triggers on rather than adopting the identifier without applying it.
    let mut sync = BatchSync {
        marker: initial_batch_marker,
        capacity_generation: applied_capacity.load().generation,
        knockout_deferred: boot_persistent_deferred,
        parking_batch: initial_batch_marker,
        reentries_first: true,
        attempted_batch_id: None,
        cooldowns: boot_cooldowns,
        started: 0,
        accepted: 0,
        deferred: 0,
        unstarted: 0,
    };
    loop {
        let capacity_epoch = applied_capacity.load();
        maintenance_tick(
            &live,
            &paper_state,
            &client,
            &base_url,
            &anon_key,
            &secret_key,
            &writer_lock,
            &applied_capacity,
            &preparer,
            &cfg,
            capacity_epoch,
            &mut evicted,
            &mut sync,
            OffsetDateTime::now_utc().unix_timestamp(),
        )
        .await;
        tokio::time::sleep(interval).await;
    }
}

/// Load per-wallet edge stats from the authoritative local paper-state. `None` (with a
/// warn) on any read failure — the knockout pass skips its tick; the full-rerank audit
/// degrades to stat-less rows.
struct LoadedEdgeStats {
    by_wallet: HashMap<String, WalletEdgeStats>,
    fills: Vec<FillRow>,
    resolutions: ResolutionStore,
}

fn load_edge_stats(
    paper_state: &Arc<PaperStateDb>,
    cfg: &MaintenanceConfig,
    now_unix: i64,
) -> Option<LoadedEdgeStats> {
    let fills = match paper_state.list_fills() {
        Ok(f) => f,
        Err(e) => {
            warn!(error = %e, "maintenance: list_fills failed");
            return None;
        }
    };
    let resolutions = match ResolutionStore::load(Arc::clone(paper_state)) {
        Ok(r) => r,
        Err(e) => {
            warn!(error = %e, "maintenance: resolution-store load failed");
            return None;
        }
    };
    let by_wallet = wallet_edge_stats(
        &fills,
        &resolutions,
        cfg.demotion_cb_alpha,
        now_unix,
        cfg.demotion_pnl_window_secs,
    );
    Some(LoadedEdgeStats {
        by_wallet,
        fills,
        resolutions,
    })
}

/// One maintenance pass. Best-effort throughout: any single failure (batch fetch, list_fills,
/// candidate fetch, lifecycle write) is logged and the tick degrades rather than panicking.
#[allow(clippy::too_many_arguments)]
async fn maintenance_tick(
    live: &LiveWatchlist,
    paper_state: &Arc<PaperStateDb>,
    client: &reqwest::Client,
    base_url: &str,
    anon_key: &str,
    secret_key: &str,
    writer_lock: &Mutex<()>,
    applied_capacity: &AppliedWatchlistCapacity,
    preparer: &AdmissionPreparer,
    cfg: &MaintenanceConfig,
    capacity_epoch: WatchlistCapacityEpoch,
    evicted: &mut HashSet<WalletAddress>,
    sync: &mut BatchSync,
    now_unix: i64,
) {
    let started = tokio::time::Instant::now();
    let deadline = started + Duration::from_secs(cfg.interval_secs);
    sync.started = 0;
    sync.accepted = 0;
    sync.deferred = 0;
    sync.unstarted = 0;
    sync.attempted_batch_id = None;
    maintenance_tick_inner(
        live,
        paper_state,
        client,
        base_url,
        anon_key,
        secret_key,
        writer_lock,
        applied_capacity,
        preparer,
        cfg,
        capacity_epoch,
        evicted,
        sync,
        now_unix,
        deadline,
    )
    .await;
    info!(
        elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        deadline_expired = tokio::time::Instant::now() >= deadline,
        started = sync.started,
        accepted = sync.accepted,
        deferred = sync.deferred,
        unstarted = sync.unstarted,
        attempted_batch_id = sync.attempted_batch_id,
        applied_batch_id = sync.marker,
        capacity_generation = capacity_epoch.generation,
        "maintenance admission budget completed"
    );
}

#[allow(clippy::too_many_arguments)]
async fn maintenance_tick_inner(
    live: &LiveWatchlist,
    paper_state: &Arc<PaperStateDb>,
    client: &reqwest::Client,
    base_url: &str,
    anon_key: &str,
    secret_key: &str,
    writer_lock: &Mutex<()>,
    applied_capacity: &AppliedWatchlistCapacity,
    preparer: &AdmissionPreparer,
    cfg: &MaintenanceConfig,
    capacity_epoch: WatchlistCapacityEpoch,
    evicted: &mut HashSet<WalletAddress>,
    sync: &mut BatchSync,
    now_unix: i64,
    deadline: tokio::time::Instant,
) {
    let cap = capacity_epoch.target;
    let mut held_batch: Option<(i64, Watchlist, HashMap<WalletAddress, i64>)> = None;
    let mut attempted_reentries = HashSet::new();
    sync.reentries_first = !sync.reentries_first;
    if sync.reentries_first {
        let report = live_reentry_tick(
            live,
            paper_state,
            cfg,
            client,
            base_url,
            anon_key,
            secret_key,
            writer_lock,
            applied_capacity,
            capacity_epoch,
            preparer,
            sync,
            None,
            &mut attempted_reentries,
            now_unix,
            Some(deadline),
        )
        .await;
        record_live_reentry(preparer, sync, cfg.membership_mode, report).await;
    }
    let mut shared_batch_fetch_failed = false;
    // A capacity transition since the last sync means membership may reflect an older
    // `latest_ranking` read than the batch this loop last applied (#542).
    let capacity_changed = capacity_epoch.generation != sync.capacity_generation;

    // Ranking-batch step remains unconditional, independent of re-entry/read health.
    // Knockout mode: a fresh batch clears the evicted-set and can restore prepared,
    // structural wallets that are absent from live. FullRerank mode: a batch TRANSITION
    // hands membership to the ranker —
    // wholesale swap to the new top-`cap`; the marker only advances on a successful swap
    // so a failed fetch retries next tick. Audit stats for dropped wallets are best-effort
    // decoration: a list_fills failure degrades the audit rows, never blocks the swap.
    match supabase_reader::fetch_latest_batch_id(client, base_url, anon_key, secret_key).await {
        Ok(latest) => {
            if latest.is_some() && latest != sync.parking_batch {
                sync.knockout_deferred.clear();
                sync.parking_batch = latest;
            }
            match cfg.membership_mode {
                MembershipMode::Knockout => {
                    if let Some(batch_id) = latest
                        && latest != sync.marker
                    {
                        sync.attempted_batch_id = Some(batch_id);
                        match supabase_reader::fetch_batch(
                            client,
                            base_url,
                            anon_key,
                            secret_key,
                            batch_id,
                            MAX_ACTIVE_WATCHLIST_SIZE,
                        )
                        .await
                        {
                            Ok((incoming, incoming_last_trade)) => {
                                match paper_state.wallet_fences() {
                                    Ok(_) => {
                                        let fetched = (
                                            batch_id,
                                            incoming.clone(),
                                            incoming_last_trade.clone(),
                                        );
                                        // Knockout keeps structural membership. The live-only reentry
                                        // below checks fences under the writer lock using these pinned rows.
                                        let _writer = writer_lock.lock().await;
                                        if applied_capacity.load() == capacity_epoch {
                                            if sync.marker.is_some() {
                                                evicted.clear();
                                            }
                                            sync.knockout_deferred.clear();
                                            sync.marker = Some(batch_id);
                                            held_batch = Some(fetched);
                                        } else {
                                            warn!(
                                                batch_id,
                                                "knockout: capacity changed during live reentry preparation; retrying batch"
                                            );
                                        }
                                    }
                                    Err(error) => warn!(%error, batch_id,
                                    "knockout: fence read failed; keeping batch marker for retry"),
                                }
                            }
                            Err(error) => {
                                shared_batch_fetch_failed = true;
                                warn!(%error, batch_id,
                                "knockout: pinned batch fetch failed; keeping batch marker for retry");
                            }
                        }
                    }
                    // Knockout structural membership is never batch-applied, so there is no
                    // capacity-driven re-sync.
                    sync.capacity_generation = capacity_epoch.generation;
                }
                MembershipMode::FullRerank => {
                    // Every transition applies the batch it triggered on — including the first tick
                    // after a failed boot batch read (#542). The pinned `ranking_entries` read binds
                    // the rows, the preparation, and the committed marker to one batch identifier;
                    // the moving `latest_ranking` view could otherwise return a newer batch's rows.
                    if let Some(batch_id) = latest
                        && (latest != sync.marker || capacity_changed)
                    {
                        sync.attempted_batch_id = Some(batch_id);
                        match supabase_reader::fetch_batch(
                            client,
                            base_url,
                            anon_key,
                            secret_key,
                            batch_id,
                            MAX_ACTIVE_WATCHLIST_SIZE,
                        )
                        .await
                        {
                            Ok((incoming, incoming_last_trade)) => 'replacement: {
                                let fetched =
                                    (batch_id, incoming.clone(), incoming_last_trade.clone());
                                let fenced =
                                    match crate::position_seeder::unrecoverable_fenced_wallets(
                                        paper_state,
                                    ) {
                                        Ok(wallets) => wallets,
                                        Err(error) => {
                                            warn!(%error, "full_rerank: fence read failed; keeping batch marker for retry");
                                            break 'replacement;
                                        }
                                    };
                                let mut excluded = fenced;
                                excluded.extend(sync.knockout_deferred.iter().copied());
                                let retained = live.structural_membership();
                                excluded.extend(
                                    incoming
                                        .entries
                                        .iter()
                                        .filter(|entry| {
                                            !retained.contains(&entry.wallet)
                                                && sync.cooling(&entry.wallet)
                                        })
                                        .map(|entry| entry.wallet),
                                );
                                let mut prepared = HashSet::new();
                                let mut recaptured = HashSet::new();
                                let mut deferrals = Vec::new();
                                let (incoming, additions, live_total, dropped, paper_receipt) = loop {
                                    let plan = match plan_membership(
                                        live,
                                        preparer,
                                        &incoming,
                                        &incoming_last_trade,
                                        excluded.clone(),
                                        cap,
                                        cap,
                                        None,
                                        &mut prepared,
                                        Some(deadline),
                                        Some(sync),
                                    )
                                    .await
                                    {
                                        Ok(plan) => plan,
                                        Err(abort) => {
                                            deferrals.extend(abort.deferrals);
                                            error!(batch_id, kind = abort.kind, cause = %abort.message, "full_rerank: shared admission failure");
                                            park_persistent(sync, paper_state, &deferrals);
                                            preparer.record_deferrals(crate::watchlist_admission::DeferralContext::FullRerank { batch_id }, deferrals, crate::watchlist_admission::DeferralOutcome::AbortedShared { kind: abort.kind }).await;
                                            break 'replacement;
                                        }
                                    };
                                    for deferral in &plan.deferrals {
                                        excluded.insert(deferral.wallet);
                                    }
                                    deferrals.extend(plan.deferrals);
                                    let ranking_receipt = match preparer
                                        .record_ranking_membership(
                                            Some(batch_id),
                                            plan.entries.clone(),
                                        )
                                        .await
                                    {
                                        Ok(receipt) => receipt,
                                        Err(error) => {
                                            error!(batch_id, kind = error.kind(), %error, "full_rerank: ranking artifact failed");
                                            park_persistent(sync, paper_state, &deferrals);
                                            preparer.record_deferrals(crate::watchlist_admission::DeferralContext::FullRerank { batch_id }, deferrals, crate::watchlist_admission::DeferralOutcome::AbortedShared { kind: error.kind() }).await;
                                            break 'replacement;
                                        }
                                    };
                                    let admission_receipts = match preparer
                                        .record_admission_proofs(&plan.proofs)
                                        .await
                                    {
                                        Ok(receipts) => receipts,
                                        Err(error) => {
                                            error!(batch_id, kind = error.kind(), %error, "full_rerank: admission artifact failed");
                                            park_persistent(sync, paper_state, &deferrals);
                                            preparer.record_deferrals(crate::watchlist_admission::DeferralContext::FullRerank { batch_id }, deferrals, crate::watchlist_admission::DeferralOutcome::AbortedShared { kind: error.kind() }).await;
                                            break 'replacement;
                                        }
                                    };
                                    let evidence = match SealedMembershipEvidence::full_rerank(
                                        ranking_receipt,
                                        admission_receipts,
                                    ) {
                                        Ok(evidence) => evidence,
                                        Err(error) => {
                                            error!(batch_id, %error, "full_rerank: membership evidence failed");
                                            park_persistent(sync, paper_state, &deferrals);
                                            preparer.record_deferrals(crate::watchlist_admission::DeferralContext::FullRerank { batch_id }, deferrals, crate::watchlist_admission::DeferralOutcome::AbortedShared { kind: "evidence.encoding" }).await;
                                            break 'replacement;
                                        }
                                    };
                                    match apply_full_rerank_swap(
                                        live,
                                        paper_state,
                                        writer_lock,
                                        preparer,
                                        MembershipPublication {
                                            reason: MembershipReason::FullRerank,
                                            ranking_batch_id: Some(batch_id),
                                            evidence,
                                            binding: plan.binding,
                                        },
                                        applied_capacity,
                                        capacity_epoch,
                                        &plan.entries,
                                        &plan.last_trade,
                                        &[],
                                    )
                                    .await
                                    {
                                        Ok((live_total, dropped, receipt)) => {
                                            let mut selected = incoming.clone();
                                            selected.entries = plan.entries;
                                            break (
                                                selected,
                                                plan.additions,
                                                live_total,
                                                dropped,
                                                receipt,
                                            );
                                        }
                                        Err(MembershipApplyError::Publication(
                                            PublishError::Wallet {
                                                wallet,
                                                cause: WalletPublishCause::ProofChanged,
                                            },
                                        )) if recaptured.insert(wallet) => continue,
                                        Err(error)
                                            if error.class()
                                                != crate::position_seeder::FailureClass::Shared =>
                                        {
                                            if let Some((wallet, kind)) = error.deferrable_wallet()
                                            {
                                                excluded.insert(wallet);
                                                deferrals.push(
                                                    crate::watchlist_admission::Deferral {
                                                        completed_at: Some(
                                                            tokio::time::Instant::now(),
                                                        ),
                                                        wallet,
                                                        stage: "publication",
                                                        class: error.class(),
                                                        kind,
                                                        message: error.to_string(),
                                                    },
                                                );
                                                park_persistent(sync, paper_state, &deferrals);
                                                continue;
                                            }
                                            error!(batch_id, %error, "full_rerank: unlocated wallet failure");
                                            break 'replacement;
                                        }
                                        Err(MembershipApplyError::Publication(
                                            PublishError::UncertainAppend(message),
                                        )) => {
                                            error!(batch_id, %message, "full_rerank: paper append outcome uncertain");
                                            break 'replacement;
                                        }
                                        Err(error) => {
                                            error!(batch_id, kind = error.kind(), %error, "full_rerank: shared publication failure");
                                            park_persistent(sync, paper_state, &deferrals);
                                            preparer.record_deferrals(crate::watchlist_admission::DeferralContext::FullRerank { batch_id }, deferrals, crate::watchlist_admission::DeferralOutcome::AbortedShared { kind: error.kind() }).await;
                                            break 'replacement;
                                        }
                                    }
                                };
                                let outcome = paper_receipt.map_or(
                                    crate::watchlist_admission::DeferralOutcome::NoChange,
                                    |receipt| {
                                        crate::watchlist_admission::DeferralOutcome::Published {
                                            paper_seq: receipt.sequence.0,
                                        }
                                    },
                                );
                                info!(
                                    batch_id,
                                    admitted = additions.len(),
                                    deferred = deferrals.len(),
                                    "full_rerank: admission attempt completed"
                                );

                                park_persistent(sync, paper_state, &deferrals);
                                preparer
                                    .record_deferrals(
                                        crate::watchlist_admission::DeferralContext::FullRerank {
                                            batch_id,
                                        },
                                        deferrals,
                                        outcome,
                                    )
                                    .await;
                                let audit_stats = load_edge_stats(paper_state, cfg, now_unix);
                                for w in &dropped {
                                    let s = audit_stats
                                        .as_ref()
                                        .and_then(|loaded| loaded.by_wallet.get(&w.to_string()));
                                    if let Err(e) = supabase_reader::write_lifecycle_event(
                                        client,
                                        base_url,
                                        anon_key,
                                        secret_key,
                                        &w.to_string(),
                                        KnockoutReason::RankerRotation.reason_text(),
                                        s.map(|st| st.realized_pnl),
                                        i64::try_from(s.map_or(0, |st| st.settled_count))
                                            .unwrap_or(i64::MAX),
                                        paper_state.cursor(w).unwrap_or(None),
                                    )
                                    .await
                                    {
                                        warn!(wallet = %w, error = %e,
                                        "full_rerank: lifecycle write failed (best-effort)");
                                    }
                                }
                                // Memoryless by design: the ranker's verdict overrides demotion
                                // memory at each batch transition; the knockout resumes next tick.
                                // A capacity re-sync of the same batch is not a transition and
                                // keeps this batch's eviction memory.
                                if latest != sync.marker {
                                    evicted.clear();
                                }
                                sync.marker = latest;
                                sync.capacity_generation = capacity_epoch.generation;
                                held_batch = Some(fetched);
                                if incoming.entries.is_empty() {
                                    // #518 made this reachable in normal operation: the read is
                                    // survivor-filtered, so a batch whose rows all fail the gate —
                                    // or one that carries no verdict at all — legitimately returns
                                    // zero rows. Retaining the previous set would keep copying
                                    // wallets the CURRENT batch says are ineligible. Applying the
                                    // empty membership matches the cold-boot stance (`main.rs`
                                    // refuses to start on an empty filtered read) and the
                                    // fail-closed contract. Open positions keep resolving; only
                                    // new copies stop.
                                    warn!(
                                        batch_id,
                                        dropped = dropped.len(),
                                        live_total,
                                        "full_rerank: batch has no surviving rows; live set emptied \
                                     (fail-closed — the ranker endorsed nobody)"
                                    );
                                } else {
                                    info!(
                                        batch_id,
                                        admitted = additions.len(),
                                        dropped = dropped.len(),
                                        live_total,
                                        "full re-rank membership swap applied"
                                    );
                                }
                                let report = live_reentry_tick(
                                    live,
                                    paper_state,
                                    cfg,
                                    client,
                                    base_url,
                                    anon_key,
                                    secret_key,
                                    writer_lock,
                                    applied_capacity,
                                    capacity_epoch,
                                    preparer,
                                    sync,
                                    held_batch.as_ref(),
                                    &mut attempted_reentries,
                                    now_unix,
                                    Some(deadline),
                                )
                                .await;
                                record_live_reentry(preparer, sync, cfg.membership_mode, report)
                                    .await;
                                return;
                            }
                            Err(e) => {
                                shared_batch_fetch_failed = true;
                                warn!(error = %e, batch_id,
                                "full_rerank: pinned batch fetch failed; keeping membership, will retry next tick");
                            }
                        }
                    }
                }
            }
        }
        Err(e) => {
            shared_batch_fetch_failed = true;
            warn!(error = %e, "maintenance: batch-id fetch failed; keeping evicted-set");
        }
    }

    // Give additions the shared preparation budget first on their alternating ticks. Returning
    // from the knockout pass cannot skip the independently required live re-entry call.
    if !sync.reentries_first {
        knockout_tick(
            live,
            paper_state,
            client,
            base_url,
            anon_key,
            secret_key,
            writer_lock,
            applied_capacity,
            preparer,
            cfg,
            capacity_epoch,
            evicted,
            sync,
            now_unix,
            deadline,
        )
        .await;
    }
    // Every applied batch gives structurally present, live-absent wallets one admission turn,
    // including ticks that will return on edge-stat failure or full structural capacity.
    let report = if shared_batch_fetch_failed {
        None
    } else {
        live_reentry_tick(
            live,
            paper_state,
            cfg,
            client,
            base_url,
            anon_key,
            secret_key,
            writer_lock,
            applied_capacity,
            capacity_epoch,
            preparer,
            sync,
            held_batch.as_ref(),
            &mut attempted_reentries,
            now_unix,
            Some(deadline),
        )
        .await
    };
    record_live_reentry(preparer, sync, cfg.membership_mode, report).await;

    if sync.reentries_first {
        knockout_tick(
            live,
            paper_state,
            client,
            base_url,
            anon_key,
            secret_key,
            writer_lock,
            applied_capacity,
            preparer,
            cfg,
            capacity_epoch,
            evicted,
            sync,
            now_unix,
            deadline,
        )
        .await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn knockout_tick(
    live: &LiveWatchlist,
    paper_state: &Arc<PaperStateDb>,
    client: &reqwest::Client,
    base_url: &str,
    anon_key: &str,
    secret_key: &str,
    writer_lock: &Mutex<()>,
    applied_capacity: &AppliedWatchlistCapacity,
    preparer: &AdmissionPreparer,
    cfg: &MaintenanceConfig,
    capacity_epoch: WatchlistCapacityEpoch,
    evicted: &mut HashSet<WalletAddress>,
    sync: &mut BatchSync,
    now_unix: i64,
    deadline: tokio::time::Instant,
) {
    let cap = capacity_epoch.target;
    // 2. Per-wallet edge stats from the authoritative local paper-state (knockout pass only —
    // the batch step above never depends on this succeeding).
    let Some(stats) = load_edge_stats(paper_state, cfg, now_unix) else {
        warn!("maintenance: edge-stats load failed; skipping knockout pass this tick");
        return;
    };

    // 3. Snapshot the live wallets and their cursors.
    let live_snapshot = live.snapshot();
    let live_wallets: Vec<WalletAddress> = live_snapshot.entries.iter().map(|e| e.wallet).collect();
    let mut cursors: HashMap<WalletAddress, Option<i64>> =
        HashMap::with_capacity(live_wallets.len());
    for w in &live_wallets {
        // #511: the inactivity clock is `last_activity_unix` (advanced every round even
        // while the delivery cursor is HELD below an unseen trade), falling back to the
        // cursor for unmigrated rows. A read error self-heals to `None` (just-admitted).
        let activity = paper_state
            .activity(w)
            .unwrap_or(None)
            .or_else(|| paper_state.cursor(w).unwrap_or(None));
        cursors.insert(*w, activity);
    }

    // 4. Decide evictions. Nothing to do only when there are no evictions and the set is full.
    let evictions = decide_evictions(&live_snapshot, &stats.by_wallet, &cursors, cfg, now_unix);
    let structural_wallets = live.structural_membership();
    if evictions.is_empty() && structural_wallets.len() >= cap {
        return;
    }

    // 5. Stage this tick's evictions. Commit the cross-tick memory only after the structural
    // write succeeds; a stale capacity epoch must leave both membership and policy memory intact.
    let mut next_evicted = evicted.clone();
    for ev in &evictions {
        next_evicted.insert(ev.wallet);
    }

    // 6. Fetch bench candidates for freed slots, excluding (live ∪ evicted), then atomic replace.
    let survivors = structural_wallets.len().saturating_sub(evictions.len());
    let freed = cap.saturating_sub(survivors);
    let mut backfill_excluded: HashSet<WalletAddress> = structural_wallets
        .iter()
        .copied()
        .chain(next_evicted.iter().copied())
        .chain(sync.knockout_deferred.iter().copied())
        .collect();
    backfill_excluded.extend(
        sync.cooldowns
            .iter()
            .filter(|(_, end)| tokio::time::Instant::now() < **end)
            .map(|(wallet, _)| *wallet),
    );
    match crate::position_seeder::unrecoverable_fenced_wallets(paper_state) {
        Ok(wallets) => backfill_excluded.extend(wallets),
        Err(error) => {
            warn!(%error, "maintenance: fence read failed; keeping membership for retry");
            return;
        }
    }
    let mut empty_candidates = (*live.snapshot()).clone();
    empty_candidates.entries.clear();
    empty_candidates.active_count = 0;
    empty_candidates.incubator_count = 0;
    let (candidates, candidate_last_trade) = if let Some(batch_id) = sync.marker
        && freed > 0
    {
        let exclude: Vec<WalletAddress> = backfill_excluded.iter().copied().collect();
        match supabase_reader::fetch_candidates(
            client,
            base_url,
            anon_key,
            secret_key,
            batch_id,
            &exclude,
            MAX_ACTIVE_WATCHLIST_SIZE,
            now_unix,
        )
        .await
        {
            Ok((w, candidate_last_trade)) => {
                if w.entries.is_empty() {
                    // Expected steady state after #518: the bench is survivor-filtered, and
                    // every survivor is already live, so there is normally nobody left to
                    // backfill with and the live set sits below `cap` until the next batch.
                    // Carry the counts so an UNEXPECTED empty bench stays diagnosable.
                    warn!(
                        freed,
                        live_total = live_wallets.len(),
                        evicted = evictions.len(),
                        "maintenance: candidate fetch returned 0; backfill paused (no surviving \
                         bench rows outside the live set, or the bench predates last_trade_unix)"
                    );
                }
                (w, candidate_last_trade)
            }
            Err(e) => {
                warn!(error = %e, "maintenance: candidate fetch failed; evicting without backfill");
                (empty_candidates.clone(), HashMap::new())
            }
        }
    } else {
        (empty_candidates.clone(), HashMap::new())
    };

    // Keep the complete fetched bench so a deferred leading wallet can be replaced by the next
    // survivor. A shared backfill failure preserves the independently decided evictions.
    let removed: HashSet<WalletAddress> = next_evicted.iter().copied().collect();
    let mut prepared = HashSet::new();
    let mut recaptured = HashSet::new();
    let mut deferrals = Vec::new();
    let mut backfill_shared = false;

    let Some(knockout_inputs) = evictions
        .iter()
        .map(|eviction| {
            knockout_causal_input(eviction, cfg, now_unix, &stats.fills, &stats.resolutions)
        })
        .collect::<Option<Vec<_>>>()
    else {
        warn!(
            "maintenance: decided eviction lacks its typed causal statistic; keeping membership and eviction memory"
        );
        return;
    };
    let knockout_evictions = match preparer.record_knockout_inputs(knockout_inputs).await {
        Ok(evidence) => evidence,
        Err(error) => {
            warn!(%error,
                "maintenance: knockout causal evidence recording failed; keeping membership and eviction memory");
            return;
        }
    };
    let empty_last_trade = HashMap::new();
    let (live_total, paper_receipt, admitted) = loop {
        let source = if backfill_shared {
            &empty_candidates
        } else {
            &candidates
        };
        let source_last_trade = if backfill_shared {
            &empty_last_trade
        } else {
            &candidate_last_trade
        };
        let plan = match plan_membership(
            live,
            preparer,
            source,
            source_last_trade,
            backfill_excluded.clone(),
            freed,
            cap,
            Some(&removed),
            &mut prepared,
            Some(deadline),
            Some(sync),
        )
        .await
        {
            Ok(plan) => plan,
            Err(abort) if !backfill_shared => {
                park_persistent(sync, paper_state, &abort.deferrals);
                deferrals.extend(abort.deferrals);
                error!(kind = abort.kind, cause = %abort.message, "maintenance: shared backfill failure; publishing evictions only");
                backfill_shared = true;
                continue;
            }
            Err(abort) => {
                error!(kind = abort.kind, cause = %abort.message, "maintenance: eviction-only planning failed");
                park_persistent(sync, paper_state, &abort.deferrals);
                deferrals.extend(abort.deferrals);
                audit_knockout_abort(preparer, sync.marker, deferrals, abort.kind).await;
                return;
            }
        };
        for deferral in &plan.deferrals {
            backfill_excluded.insert(deferral.wallet);
        }
        park_persistent(sync, paper_state, &plan.deferrals);
        deferrals.extend(plan.deferrals);
        let ranking_receipt = if plan.entries.is_empty() {
            None
        } else {
            match preparer
                .record_ranking_membership(sync.marker, plan.entries.clone())
                .await
            {
                Ok(receipt) => Some(receipt),
                Err(error) => {
                    warn!(%error, "maintenance: knockout ranking artifact failed");
                    audit_knockout_abort(preparer, sync.marker, deferrals, error.kind()).await;
                    return;
                }
            }
        };
        let admission_receipts = match preparer.record_admission_proofs(&plan.proofs).await {
            Ok(receipts) => receipts,
            Err(error) => {
                warn!(%error, "maintenance: knockout admission artifact failed");
                audit_knockout_abort(preparer, sync.marker, deferrals, error.kind()).await;
                return;
            }
        };
        let evidence = match SealedMembershipEvidence::knockout_backfill(
            knockout_evictions.clone(),
            ranking_receipt,
            admission_receipts,
        ) {
            Ok(evidence) => evidence,
            Err(error) => {
                warn!(%error, "maintenance: knockout evidence encoding failed");
                audit_knockout_abort(preparer, sync.marker, deferrals, "evidence.encoding").await;
                return;
            }
        };
        let reason = evictions
            .iter()
            .map(|eviction| eviction.reason)
            .find(|reason| *reason == KnockoutReason::Underperformance)
            .or_else(|| {
                evictions
                    .iter()
                    .map(|eviction| eviction.reason)
                    .find(|reason| *reason == KnockoutReason::InactivityHardCap)
            })
            .or_else(|| evictions.first().map(|eviction| eviction.reason))
            .map_or(MembershipReason::KnockoutInactivity, Into::into);
        match apply_evictions_and_backfill(
            live,
            paper_state,
            writer_lock,
            preparer,
            MembershipPublication {
                reason,
                ranking_batch_id: sync.marker,
                evidence,
                binding: plan.binding,
            },
            applied_capacity,
            capacity_epoch,
            &removed,
            &plan.entries,
            &plan.last_trade,
        )
        .await
        {
            Ok((total, receipt)) => break (total, receipt, plan.additions.len()),
            Err(MembershipApplyError::Publication(PublishError::Wallet {
                wallet,
                cause: WalletPublishCause::ProofChanged,
            })) if recaptured.insert(wallet) => continue,
            Err(error) if error.class() != crate::position_seeder::FailureClass::Shared => {
                if let Some((wallet, kind)) = error.deferrable_wallet() {
                    backfill_excluded.insert(wallet);
                    if error.class() == crate::position_seeder::FailureClass::WalletPersistent
                        && !crate::position_seeder::recoverable_fence_failure(
                            paper_state,
                            &wallet,
                            kind,
                        )
                    {
                        sync.knockout_deferred.insert(wallet);
                    }
                    deferrals.push(crate::watchlist_admission::Deferral {
                        completed_at: Some(tokio::time::Instant::now()),
                        wallet,
                        stage: "publication",
                        class: error.class(),
                        kind,
                        message: error.to_string(),
                    });
                    park_persistent(sync, paper_state, &deferrals);
                    continue;
                }
                warn!(%error, "maintenance: unlocated wallet publication failure");
                audit_knockout_abort(preparer, sync.marker, deferrals, error.kind()).await;
                return;
            }
            Err(MembershipApplyError::Publication(PublishError::UncertainAppend(message))) => {
                error!(%message, "maintenance: paper append outcome uncertain");
                return;
            }
            Err(error) => {
                error!(kind = error.kind(), %error, "maintenance: shared publication failure");
                audit_knockout_abort(preparer, sync.marker, deferrals, error.kind()).await;
                return;
            }
        }
    };
    let outcome = paper_receipt.map_or(
        crate::watchlist_admission::DeferralOutcome::NoChange,
        |receipt| crate::watchlist_admission::DeferralOutcome::Published {
            paper_seq: receipt.sequence.0,
        },
    );
    info!(batch_id = ?sync.marker, admitted, deferred = deferrals.len(), "maintenance: knockout admission attempt completed");
    if let Some(batch_id) = sync.marker {
        park_persistent(sync, paper_state, &deferrals);
        preparer
            .record_deferrals(
                crate::watchlist_admission::DeferralContext::Knockout { batch_id },
                deferrals,
                outcome,
            )
            .await;
    }
    *evicted = next_evicted;

    // 7. Best-effort lifecycle audit rows for each eviction this tick.
    for ev in &evictions {
        if let Err(e) = supabase_reader::write_lifecycle_event(
            client,
            base_url,
            anon_key,
            secret_key,
            &ev.wallet.to_string(),
            ev.reason.reason_text(),
            ev.live_pnl,
            i64::try_from(ev.trades_observed).unwrap_or(i64::MAX),
            ev.last_trade_unix,
        )
        .await
        {
            warn!(wallet = %ev.wallet, error = %e, "maintenance: lifecycle write failed (best-effort)");
        }
    }

    info!(
        evicted = evictions.len(),
        live_total, "maintenance tick applied"
    );
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use tokio::sync::mpsc;

    use crate::orchestrator_control::OrchestratorControl;
    use crate::paper_recovery::{KnockoutCausalArtifact, SealedKnockoutEvidence};
    use crate::watchlist_admission::{
        KNOCKOUT_CAUSAL_SOURCE_ID, MEMBERSHIP_ARTIFACT_PARSER_VERSION,
        MEMBERSHIP_ARTIFACT_SCHEMA_VERSION, RANKING_MEMBERSHIP_SOURCE_ID,
    };

    fn cfg() -> MaintenanceConfig {
        MaintenanceConfig {
            interval_secs: 600,
            inactivity_threshold_secs: 259_200, // 72 h
            inactivity_hard_cap_secs: 604_800,  // 7 d
            demotion_min_trades: 10,
            demotion_cb_alpha: dec!(0.10),
            demotion_pnl_window_secs: 2_592_000, // 30 d
            membership_mode: MembershipMode::default(),
        }
    }

    /// `pnl` seeds BOTH the lifetime and the windowed sum — tests that need them to
    /// diverge construct `WalletEdgeStats` directly.
    fn stats(
        settled: usize,
        pnl: Decimal,
        lower: Option<Decimal>,
        upper: Option<Decimal>,
    ) -> WalletEdgeStats {
        WalletEdgeStats {
            settled_count: settled,
            realized_pnl: pnl,
            windowed_pnl: pnl,
            lower_cb: lower,
            upper_cb: upper,
        }
    }

    const NOW: i64 = 1_900_000_000;

    fn append_artifact<T: serde::Serialize>(
        source_log: &std::path::Path,
        source_id: &str,
        artifact: &T,
    ) -> pe_event_log::AppendReceipt {
        let at = time::OffsetDateTime::from_unix_timestamp(NOW).unwrap();
        let mut writer = pe_event_log::Writer::open(source_log).unwrap();
        writer
            .append_synced(pe_event_log::EnvelopeIn {
                source_id: pe_core_types::SourceId(source_id.to_owned()),
                schema_version: MEMBERSHIP_ARTIFACT_SCHEMA_VERSION,
                parser_version: MEMBERSHIP_ARTIFACT_PARSER_VERSION,
                observed_at: pe_core_types::SourceTimestamp(at),
                received_at: pe_core_types::ReceivedAt(at),
                content_type: pe_event_log::ContentType::Json,
                payload: serde_json::to_vec(artifact).unwrap(),
            })
            .unwrap()
    }

    fn settled_edge_artifact(
        wallet: WalletAddress,
        winner: bool,
        last_trade_unix: Option<i64>,
    ) -> KnockoutCausalArtifact {
        let quantity = pe_core_types::ShareAmount::from_whole(100).unwrap();
        let principal = pe_core_types::CollateralAmount::from_decimal_exact(dec!(50)).unwrap();
        let mut fills = Vec::new();
        let mut settlements = Vec::new();
        for index in 0..10u64 {
            let market_id = pe_core_types::MarketId(pe_core_types::VenueMarketId(format!(
                "membership-proof-{index}"
            )));
            fills.push(KnockoutFillArtifact::from_row(&FillRow {
                idempotency_key: format!(
                    "wf|{wallet}|g2:{}|{}|0|buy|{}",
                    "a".repeat(64),
                    market_id.0.0,
                    NOW - 10
                ),
                market_id: market_id.clone(),
                outcome_id: pe_core_types::OutcomeId(0),
                side: pe_core_types::Side::Buy,
                quantity,
                fill_price: pe_core_types::Price::new(dec!(0.5)).unwrap(),
                principal,
                fee: pe_core_types::CollateralAmount::ZERO,
                event_seq: pe_core_types::EventSeq(index + 1),
                prepared_seq: pe_core_types::EventSeq(index + 1),
                source_receipt_seq: None,
            }));
            settlements.push(KnockoutSettlementArtifact {
                market_id,
                outcome_prices: if winner {
                    vec![Decimal::ONE, Decimal::ZERO]
                } else {
                    vec![Decimal::ZERO, Decimal::ONE]
                },
                credit_applied: Decimal::ZERO,
                settled_at_unix: NOW - 1,
            });
        }
        KnockoutCausalArtifact {
            wallet,
            evaluated_at_unix: NOW,
            last_trade_unix,
            inactivity_threshold_secs: 259_200,
            inactivity_hard_cap_secs: 604_800,
            demotion_min_trades: 10,
            demotion_cb_alpha: dec!(0.10),
            demotion_pnl_window_secs: 2_592_000,
            fills,
            settlements,
        }
    }

    async fn publish_and_verify(
        source_log: std::path::PathBuf,
        change: MembershipChange,
        replacements: Vec<WatchlistEntry>,
    ) {
        let temp = tempfile::tempdir().unwrap();
        let paper_state = Arc::new(PaperStateDb::open(&temp.path().join("paper.db")).unwrap());
        let (control_tx, mut control_rx) = mpsc::channel(1);
        let verifier_source_log = source_log.clone();
        let control = tokio::spawn(async move {
            let command = control_rx.recv().await.unwrap();
            let OrchestratorControl::PublishMembership {
                change,
                acknowledged,
                ..
            } = command
            else {
                panic!("membership publisher sent a non-publication command");
            };
            // The round-trip helpers publish full replacements: the membership before the
            // change is exactly the removed set.
            let current_membership: HashSet<WalletAddress> =
                change.removed.iter().copied().collect();
            let record = change.into_record();
            let result = crate::qualification::verify_published_membership_change(
                &record,
                &verifier_source_log,
                &current_membership,
            )
            .map(|()| pe_event_log::AppendReceipt {
                sequence: pe_core_types::EventSeq(1),
                this_hash: blake3::hash(b"membership-round-trip"),
            })
            .map_err(|error| PublishError::Shared(error.to_string()));
            acknowledged.send(result).unwrap();
        });
        AdmissionPreparer::new(control_tx, paper_state)
            .publish_membership(change, replacements, Default::default())
            .await
            .unwrap();
        control.await.unwrap();
    }

    #[tokio::test]
    async fn full_rerank_publication_round_trips_membership_verifier() {
        let temp = tempfile::tempdir().unwrap();
        let source_log = temp.path().join("source.log");
        let removed = WalletAddress([1; 20]);
        let replacements = Vec::new();
        let ranking_receipt = append_artifact(
            &source_log,
            RANKING_MEMBERSHIP_SOURCE_ID,
            &crate::paper_recovery::RankingMembershipArtifact {
                batch_id: Some(7),
                entries: replacements.clone(),
            },
        );
        publish_and_verify(
            source_log,
            MembershipChange {
                reason: MembershipReason::FullRerank,
                removed: vec![removed],
                added: Vec::new(),
                capacity: 1,
                ranking_batch_id: Some(7),
                evidence: SealedMembershipEvidence::full_rerank(ranking_receipt, Vec::new())
                    .unwrap(),
            },
            replacements,
        )
        .await;
    }

    #[tokio::test]
    async fn inactivity_publication_round_trips_membership_verifier() {
        let temp = tempfile::tempdir().unwrap();
        let source_log = temp.path().join("source.log");
        let removed = WalletAddress([5; 20]);
        let causal_receipt = append_artifact(
            &source_log,
            KNOCKOUT_CAUSAL_SOURCE_ID,
            &KnockoutCausalArtifact {
                wallet: removed,
                evaluated_at_unix: NOW,
                last_trade_unix: Some(NOW - 259_200),
                inactivity_threshold_secs: 259_200,
                inactivity_hard_cap_secs: 604_800,
                demotion_min_trades: 10,
                demotion_cb_alpha: dec!(0.10),
                demotion_pnl_window_secs: 2_592_000,
                fills: Vec::new(),
                settlements: Vec::new(),
            },
        );
        publish_and_verify(
            source_log,
            MembershipChange {
                reason: MembershipReason::KnockoutInactivity,
                removed: vec![removed],
                added: Vec::new(),
                capacity: 1,
                ranking_batch_id: Some(9),
                evidence: SealedMembershipEvidence::knockout_backfill(
                    vec![SealedKnockoutEvidence {
                        wallet: removed,
                        reason: MembershipReason::KnockoutInactivity,
                        causal_receipt,
                    }],
                    None,
                    Vec::new(),
                )
                .unwrap(),
            },
            Vec::new(),
        )
        .await;
    }

    #[tokio::test]
    async fn inactivity_hard_cap_publication_round_trips_membership_verifier() {
        let temp = tempfile::tempdir().unwrap();
        let source_log = temp.path().join("source.log");
        let removed = WalletAddress([6; 20]);
        let causal_receipt = append_artifact(
            &source_log,
            KNOCKOUT_CAUSAL_SOURCE_ID,
            &settled_edge_artifact(removed, true, Some(NOW - 604_800)),
        );
        publish_and_verify(
            source_log,
            MembershipChange {
                reason: MembershipReason::KnockoutInactivityHardCap,
                removed: vec![removed],
                added: Vec::new(),
                capacity: 1,
                ranking_batch_id: Some(10),
                evidence: SealedMembershipEvidence::knockout_backfill(
                    vec![SealedKnockoutEvidence {
                        wallet: removed,
                        reason: MembershipReason::KnockoutInactivityHardCap,
                        causal_receipt,
                    }],
                    None,
                    Vec::new(),
                )
                .unwrap(),
            },
            Vec::new(),
        )
        .await;
    }

    #[tokio::test]
    async fn underperformance_publication_round_trips_membership_verifier() {
        let temp = tempfile::tempdir().unwrap();
        let source_log = temp.path().join("source.log");
        let removed = WalletAddress([7; 20]);
        let causal_receipt = append_artifact(
            &source_log,
            KNOCKOUT_CAUSAL_SOURCE_ID,
            &settled_edge_artifact(removed, false, Some(NOW - 1)),
        );
        publish_and_verify(
            source_log,
            MembershipChange {
                reason: MembershipReason::KnockoutUnderperformance,
                removed: vec![removed],
                added: Vec::new(),
                capacity: 1,
                ranking_batch_id: Some(11),
                evidence: SealedMembershipEvidence::knockout_backfill(
                    vec![SealedKnockoutEvidence {
                        wallet: removed,
                        reason: MembershipReason::KnockoutUnderperformance,
                        causal_receipt,
                    }],
                    None,
                    Vec::new(),
                )
                .unwrap(),
            },
            Vec::new(),
        )
        .await;
    }

    #[test]
    fn no_stats_idle_under_threshold_is_kept() {
        let last = Some(NOW - 1000);
        assert_eq!(knockout_decision(last, None, &cfg(), NOW), None);
    }

    #[test]
    fn none_cursor_never_inactive_evicted() {
        // No cursor (not yet polled) → idle 0 → kept, even with no stats.
        assert_eq!(knockout_decision(None, None, &cfg(), NOW), None);
    }

    #[test]
    fn unproven_idle_at_threshold_is_evicted() {
        let last = Some(NOW - 259_200);
        assert_eq!(
            knockout_decision(last, None, &cfg(), NOW),
            Some(KnockoutReason::Inactivity)
        );
    }

    #[test]
    fn proven_winner_spared_under_hard_cap() {
        let winner = stats(40, dec!(120), Some(dec!(0.05)), Some(dec!(0.30)));
        let last = Some(NOW - 300_000); // > 72h, < 7d
        assert_eq!(knockout_decision(last, Some(&winner), &cfg(), NOW), None);
    }

    #[test]
    fn proven_winner_evicted_past_hard_cap() {
        let winner = stats(40, dec!(120), Some(dec!(0.05)), Some(dec!(0.30)));
        let last = Some(NOW - 604_800); // == 7d
        assert_eq!(
            knockout_decision(last, Some(&winner), &cfg(), NOW),
            Some(KnockoutReason::InactivityHardCap)
        );
    }

    #[test]
    fn reentry_admits_fresh_batch_or_kept_clock_and_parks_only_bad_batch_values() {
        let wallet = WalletAddress::from_hex("0x00000000000000000000000000000000000000aa").unwrap();
        let by_wallet = |edge: WalletEdgeStats| HashMap::from([(wallet.to_string(), edge)]);
        let winner = by_wallet(stats(40, dec!(120), Some(dec!(0.05)), Some(dec!(0.30))));
        let loser = by_wallet(stats(20, dec!(-50), Some(dec!(-0.30)), Some(dec!(-0.05))));
        let none = HashMap::new();
        let (recent, stale, future) = (Some(NOW - 60), Some(NOW - 300_000), Some(NOW + 60));
        let (four_days, eight_days) = (Some(NOW - 345_600), Some(NOW - 691_200));
        use Reentry::{Admit, Park, Retry};
        let (empty, winners, losers) = (Some(&none), Some(&winner), Some(&loser));
        let cases = [
            (recent, None, empty, Admit),        // fresh batch
            (stale, recent, empty, Admit),       // stale batch, recent clock
            (stale, stale, empty, Retry),        // both stale
            (future, recent, empty, Park),       // future batch
            (None, recent, empty, Park),         // missing batch
            (stale, future, empty, Retry),       // future clock
            (stale, future, None, Retry),        // future clock, statistics unavailable
            (stale, None, empty, Retry),         // no clock (also a read error)
            (stale, None, None, Retry),          // no clock, statistics unavailable
            (stale, four_days, winners, Admit),  // proven winner idle 4 d
            (stale, four_days, empty, Retry),    // unproven idle 4 d
            (stale, eight_days, winners, Retry), // winner past the hard cap
            (stale, recent, losers, Retry),      // demotable
            (recent, None, None, Admit),         // statistics unavailable, fresh batch
            (stale, recent, None, Retry),        // statistics unavailable, stale batch
            (future, recent, None, Park),        // statistics unavailable, future batch
        ];
        for (index, (ranked, observed, edge, expected)) in cases.into_iter().enumerate() {
            let actual = reentry(ranked, observed, edge, &wallet, &cfg(), NOW);
            assert_eq!(actual, expected, "case {index}");
        }
    }

    #[test]
    fn underperformer_demoted_regardless_of_idle() {
        let loser = stats(20, dec!(-50), Some(dec!(-0.30)), Some(dec!(-0.05)));
        let active = Some(NOW - 10); // recently active
        assert_eq!(
            knockout_decision(active, Some(&loser), &cfg(), NOW),
            Some(KnockoutReason::Underperformance)
        );
    }

    #[test]
    fn positive_pnl_never_demoted_even_with_negative_upper_cb() {
        // The windowed realized-P&L AND-gate is the safety net for the CB constants.
        let s = stats(20, dec!(0), Some(dec!(-0.30)), Some(dec!(-0.05)));
        let active = Some(NOW - 10);
        assert_eq!(knockout_decision(active, Some(&s), &cfg(), NOW), None);
    }

    #[test]
    fn lifetime_winner_bleeding_in_window_is_demoted() {
        // Behaviour change of the dollar-gate rework: lifetime P&L deep green, but the
        // trailing window is red AND the CB proves the loss → demote. Under the old
        // lifetime conjunct this wallet was shielded indefinitely.
        let s = WalletEdgeStats {
            settled_count: 40,
            realized_pnl: dec!(4110),
            windowed_pnl: dec!(-60),
            lower_cb: Some(dec!(-22)),
            upper_cb: Some(dec!(-3.5)),
        };
        let active = Some(NOW - 10);
        assert_eq!(
            knockout_decision(active, Some(&s), &cfg(), NOW),
            Some(KnockoutReason::Underperformance)
        );
    }

    #[test]
    fn membership_mode_parse_roundtrip() {
        assert_eq!(
            MembershipMode::parse("knockout"),
            Some(MembershipMode::Knockout)
        );
        assert_eq!(
            MembershipMode::parse(" Full_Rerank "),
            Some(MembershipMode::FullRerank)
        );
        assert_eq!(MembershipMode::parse("greedy"), None);
        assert_eq!(MembershipMode::default(), MembershipMode::Knockout);
    }

    #[test]
    fn incident_shape_rerank_uses_all_structural_wallets() {
        let wallet = |byte| WalletAddress([byte; 20]);
        let entry = |wallet| WatchlistEntry {
            wallet,
            tier: pe_trader_index::WatchlistTier::Active,
            leader_score_bps: pe_core_types::BasisPoints(100),
            lcb_5pct_bps: pe_core_types::BasisPoints(100),
            win_rate_bps: pe_core_types::BasisPoints(6_000),
            closed_trades_in_window: 0,
            reconstruction_quality: pe_core_types::ReconstructionQuality::new(100).unwrap(),
        };
        let structural = (1..=19).map(wallet).collect::<HashSet<_>>();
        let live = (1..=13).map(wallet).collect::<HashSet<_>>();
        let incoming = (20..=28)
            .map(|byte| entry(wallet(byte)))
            .collect::<Vec<_>>();
        let (removed, added) = ranked_membership_change_set(&structural, &incoming, 100);
        assert_eq!(removed.iter().copied().collect::<HashSet<_>>(), structural);
        assert_eq!(added.len(), 9);
        assert_eq!(live.len(), 13);
        assert!(
            planned_admission_wallets(&structural, &HashSet::new(), &[entry(wallet(19))], 100)
                .is_empty()
        );
    }

    #[test]
    fn too_few_trades_not_spared_by_inactivity() {
        // A would-be winner with < min_trades is NOT a proven winner → evicted at 72h.
        let thin = stats(9, dec!(5), Some(dec!(0.20)), Some(dec!(0.40)));
        let last = Some(NOW - 259_200);
        assert_eq!(
            knockout_decision(last, Some(&thin), &cfg(), NOW),
            Some(KnockoutReason::Inactivity)
        );
    }

    /// End-to-end `maintenance_tick` drives against a fake Supabase + Polymarket (#542): proves
    /// that every membership path prepares its exact additions through the shared preparer
    /// BEFORE publishing, and that a preparation failure never publishes an unprepared wallet.
    #[allow(clippy::panic)]
    mod tick {
        use std::sync::Mutex as StdMutex;
        use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

        use axum::extract::{Query, State};
        use axum::http::StatusCode;
        use axum::{Json, Router, routing::get};
        use pe_core_types::{BasisPoints, ReconstructionQuality, SourceTimestamp};
        use pe_source_core::SourceError;
        use pe_source_polymarket_public::{GAMMA_BATCH_SIZE, PageFetcher, ReconciliationFetcher};
        use pe_trader_index::WatchlistTier;
        use tempfile::TempDir;
        use tokio::sync::mpsc;
        use tracing_subscriber::fmt::MakeWriter;

        use super::*;
        use crate::activity_ingest::{ActivityIngest, ReconciliationTrigger, SourceLogHandle};
        use crate::asset_identity::AssetIdentityResolver;
        use crate::health::new_shared_health_with_ws;
        use crate::orchestrator_control::OrchestratorControl;
        use crate::position_seeder::CausalPositionValidator;
        use crate::source_event_sink::SourceEventSink;

        const CAP: usize = 3;

        #[derive(Clone)]
        struct CapturedLogs(Arc<StdMutex<Vec<u8>>>);

        impl std::io::Write for CapturedLogs {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .extend_from_slice(bytes);
                Ok(bytes.len())
            }

            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        impl<'a> MakeWriter<'a> for CapturedLogs {
            type Writer = Self;

            fn make_writer(&'a self) -> Self::Writer {
                self.clone()
            }
        }

        fn wallet(byte: u8) -> WalletAddress {
            WalletAddress([byte; 20])
        }

        fn row(batch_id: i64, rank: i64, wallet: WalletAddress) -> serde_json::Value {
            serde_json::json!({
                "batch_id": batch_id,
                "rank": rank,
                "wallet_hex": wallet.to_string(),
                "hit_rate": "0.60",
                "ls_tstat": "2.0",
                "n_trades": 10,
                "last_trade_unix": NOW - 60,
                "survives": true,
            })
        }

        fn entry(wallet: WalletAddress) -> WatchlistEntry {
            WatchlistEntry {
                wallet,
                tier: WatchlistTier::Active,
                leader_score_bps: BasisPoints(0),
                lcb_5pct_bps: BasisPoints(0),
                win_rate_bps: BasisPoints(0),
                closed_trades_in_window: 0,
                reconstruction_quality: ReconstructionQuality::new(100).unwrap(),
            }
        }

        fn live(wallets: &[WalletAddress]) -> LiveWatchlist {
            let entries: Vec<WatchlistEntry> = wallets.iter().copied().map(entry).collect();
            let active_count = entries.len();
            LiveWatchlist::new(Watchlist {
                entries,
                snapshot_at: SourceTimestamp(OffsetDateTime::UNIX_EPOCH),
                active_count,
                incubator_count: 0,
            })
        }

        fn members(live: &LiveWatchlist) -> HashSet<WalletAddress> {
            live.snapshot().entries.iter().map(|e| e.wallet).collect()
        }

        /// Fake Supabase + Polymarket. `ranking_entries` is served filtered by the `batch_id`
        /// query the pinned read sends; candidate queries also enforce exclusions and freshness.
        #[derive(Clone)]
        struct Fake {
            latest_batch: Arc<AtomicI64>,
            ranking_entries: Vec<serde_json::Value>,
            latest_ranking: Vec<serde_json::Value>,
            history_ok: bool,
            history_missing: HashSet<WalletAddress>,
            failure: Option<&'static str>,
            entries_fail: bool,
            activity_hits: Arc<AtomicUsize>,
            position_hits: Arc<AtomicUsize>,
            ranking_hits: Arc<AtomicUsize>,
            pinned_ranking_hits: Arc<AtomicUsize>,
        }

        impl Fake {
            fn new(latest_batch: Option<i64>) -> Self {
                Self {
                    latest_batch: Arc::new(AtomicI64::new(latest_batch.unwrap_or(-1))),
                    ranking_entries: Vec::new(),
                    latest_ranking: Vec::new(),
                    history_ok: true,
                    history_missing: HashSet::new(),
                    failure: None,
                    entries_fail: false,
                    activity_hits: Arc::new(AtomicUsize::new(0)),
                    position_hits: Arc::new(AtomicUsize::new(0)),
                    ranking_hits: Arc::new(AtomicUsize::new(0)),
                    pinned_ranking_hits: Arc::new(AtomicUsize::new(0)),
                }
            }

            async fn serve(mut self) -> String {
                if self.ranking_entries.is_empty() {
                    self.ranking_entries = self.latest_ranking.clone();
                }
                fn selected(
                    rows: &[serde_json::Value],
                    q: &HashMap<String, String>,
                ) -> Vec<serde_json::Value> {
                    assert_eq!(q.get("survives").map(String::as_str), Some("is.true"));
                    assert_eq!(q.get("order").map(String::as_str), Some("rank"));
                    let limit = q["limit"].parse::<usize>().unwrap();
                    let batch = q
                        .get("batch_id")
                        .map(|value| value.strip_prefix("eq.").unwrap().parse::<i64>().unwrap());
                    let cutoff = q
                        .get("last_trade_unix")
                        .map(|value| value.strip_prefix("gte.").unwrap().parse::<i64>().unwrap());
                    let excluded = q
                        .get("wallet_hex")
                        .map(|value| {
                            value
                                .strip_prefix("not.in.(")
                                .unwrap()
                                .strip_suffix(')')
                                .unwrap()
                                .split(',')
                                .collect::<HashSet<_>>()
                        })
                        .unwrap_or_default();
                    let mut selected = rows
                        .iter()
                        .filter(|row| {
                            row["survives"] == true
                                && batch.is_none_or(|batch| row["batch_id"].as_i64() == Some(batch))
                                && cutoff.is_none_or(|cutoff| {
                                    row["last_trade_unix"]
                                        .as_i64()
                                        .is_some_and(|time| time >= cutoff)
                                })
                                && !excluded.contains(row["wallet_hex"].as_str().unwrap())
                        })
                        .cloned()
                        .collect::<Vec<_>>();
                    selected.sort_by_key(|row| row["rank"].as_i64().unwrap());
                    selected.truncate(limit);
                    selected
                }
                async fn batches(State(fake): State<Fake>) -> Json<serde_json::Value> {
                    Json(match fake.latest_batch.load(Ordering::SeqCst) {
                        -1 => serde_json::json!([]),
                        id => serde_json::json!([{ "batch_id": id }]),
                    })
                }
                async fn entries(
                    State(fake): State<Fake>,
                    Query(q): Query<HashMap<String, String>>,
                ) -> Result<Json<Vec<serde_json::Value>>, StatusCode> {
                    fake.ranking_hits.fetch_add(1, Ordering::SeqCst);
                    if !q.contains_key("last_trade_unix") {
                        fake.pinned_ranking_hits.fetch_add(1, Ordering::SeqCst);
                    }
                    if fake.entries_fail {
                        return Err(StatusCode::INTERNAL_SERVER_ERROR);
                    }
                    let pinned = q
                        .get("batch_id")
                        .and_then(|v| v.strip_prefix("eq."))
                        .and_then(|v| v.parse::<i64>().ok())
                        .expect("pinned read must carry batch_id=eq.N");
                    assert!(pinned >= 0);
                    Ok(Json(selected(&fake.ranking_entries, &q)))
                }
                async fn latest(
                    State(fake): State<Fake>,
                    Query(q): Query<HashMap<String, String>>,
                ) -> Json<Vec<serde_json::Value>> {
                    Json(selected(&fake.latest_ranking, &q))
                }
                async fn activity(
                    State(fake): State<Fake>,
                ) -> Result<Json<Vec<serde_json::Value>>, StatusCode> {
                    fake.activity_hits.fetch_add(1, Ordering::SeqCst);
                    if fake.history_ok {
                        Ok(Json(Vec::new()))
                    } else {
                        Err(StatusCode::NOT_FOUND)
                    }
                }
                async fn positions(State(fake): State<Fake>) -> Json<Vec<serde_json::Value>> {
                    fake.position_hits.fetch_add(1, Ordering::SeqCst);
                    Json(Vec::new())
                }
                let app = Router::new()
                    .route("/rest/v1/ranking_batches", get(batches))
                    .route("/rest/v1/ranking_entries", get(entries))
                    .route("/rest/v1/latest_ranking", get(latest))
                    .route(
                        "/rest/v1/wallet_lifecycle_events",
                        axum::routing::post(|| async { StatusCode::CREATED }),
                    )
                    .route("/activity", get(activity))
                    .route("/positions", get(positions))
                    .with_state(self);
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let address = listener.local_addr().unwrap();
                std::mem::drop(tokio::spawn(async move {
                    let _ = axum::serve(listener, app).await;
                }));
                format!("http://{address}")
            }
        }

        /// `(prepared wallets, live membership when the orchestrator applied them)`.
        type ControlLog = Vec<(HashSet<WalletAddress>, HashSet<WalletAddress>)>;

        /// Everything one tick needs. The control consumer acknowledges every preparation and
        /// records `(prepared wallets, live membership at that moment)` so a test can prove
        /// the prepared wallet was not yet published.
        struct Harness {
            live: LiveWatchlist,
            projection_rx: tokio::sync::watch::Receiver<u64>,
            paper_state: Arc<PaperStateDb>,
            preparer: crate::watchlist_admission::AdmissionPreparer,
            control_tx: mpsc::Sender<OrchestratorControl>,
            source_handle: SourceLogHandle,
            applied: AppliedWatchlistCapacity,
            writer_lock: Mutex<()>,
            client: reqwest::Client,
            base_url: String,
            controls: Arc<StdMutex<ControlLog>>,
            membership_publications: Arc<AtomicUsize>,
            published_batches: Arc<StdMutex<Vec<Option<i64>>>>,
            _source_task: tokio::task::JoinHandle<()>,
            _source_triggers: mpsc::Receiver<ReconciliationTrigger>,
            _temp: TempDir,
        }

        async fn harness(fake: Fake, initial: &[WalletAddress]) -> Harness {
            // `history_ok = false` means newcomers lack complete reconciled history, so the
            // durable seed is restricted to already-member wallets — preparation must then
            // fail closed on the durable gate (`AdmissionError::MissingHistory`), never on
            // the fake transport alone.
            let history_wallets: HashSet<WalletAddress> = fake
                .ranking_entries
                .iter()
                .chain(fake.latest_ranking.iter())
                .filter_map(|row| row.get("wallet_hex").and_then(serde_json::Value::as_str))
                .filter_map(|hex| WalletAddress::from_hex(hex).ok())
                .filter(|wallet| {
                    (fake.history_ok || initial.contains(wallet))
                        && !fake.history_missing.contains(wallet)
                })
                .collect();
            let failure = fake.failure;
            let base_url = fake.serve().await;
            let temp = TempDir::new().unwrap();
            let source_log = temp.path().join("source.log");
            let source_sink = SourceEventSink::open(&source_log).unwrap();
            let (source_handle, source_rx) = SourceLogHandle::channel(16);
            let (trigger_tx, source_triggers) = mpsc::channel(1);
            let source_ingest = ActivityIngest::poll_only(
                source_sink,
                source_rx,
                trigger_tx,
                new_shared_health_with_ws(false, false, 1),
            );
            let source_task = tokio::spawn(source_ingest.run());
            let paper_state = Arc::new(PaperStateDb::open(&temp.path().join("paper.db")).unwrap());
            for wallet in history_wallets {
                paper_state
                    .record_reconciled_history_status(&pe_paper_state::WalletHistoryStatusRecord {
                        wallet,
                        complete: true,
                        proof_json: "{\"test\":true}".to_owned(),
                        updated_at_unix: NOW,
                    })
                    .unwrap();
            }
            let (dirty, projection_rx) = crate::live_watchlist::projection_dirty_channel();
            let live = LiveWatchlist::new_with_projection(
                live(initial).snapshot().as_ref().clone(),
                dirty,
            );
            let (control_tx, mut control_rx) = mpsc::channel(2);
            let controls: Arc<StdMutex<ControlLog>> = Arc::new(StdMutex::new(Vec::new()));
            let (control_live, control_log) = (live.clone(), Arc::clone(&controls));
            let membership_publications = Arc::new(AtomicUsize::new(0));
            let publication_count = Arc::clone(&membership_publications);
            let published_batches = Arc::new(StdMutex::new(Vec::new()));
            let publication_batches = Arc::clone(&published_batches);
            let fake_paper_state = Arc::clone(&paper_state);
            let verifier_source_log = source_log.clone();
            let state_path = temp.path().join("paper.db");
            std::mem::drop(tokio::spawn(async move {
                let mut engine = crate::bucket_commit::BucketCommitEngine::load(
                    Arc::clone(&fake_paper_state),
                    pe_position_ledger::PositionLedger::new(),
                )
                .unwrap();
                while let Some(message) = control_rx.recv().await {
                    match message {
                        OrchestratorControl::PrepareAdmissions {
                            wallets,
                            acknowledged,
                        } => {
                            if failure == Some("shared_prepare") {
                                drop(acknowledged);
                                continue;
                            }
                            // Mirror the real orchestrator's successful acceptance: a
                            // prepared wallet gains a current causal position validation,
                            // or the publication recheck would (correctly) reject it. The
                            // bracket itself is proven in scenario_position_bracket.rs.
                            fake_paper_state
                                .seed_cursors_if_absent(
                                    &wallets
                                        .iter()
                                        .copied()
                                        .map(|wallet| (wallet, NOW - 60))
                                        .collect::<Vec<_>>(),
                                )
                                .unwrap();
                            let installs: Vec<pe_paper_state::AnchorInstallRecord> = wallets
                                .iter()
                                .map(|wallet| pe_paper_state::AnchorInstallRecord {
                                    repaired_history: Vec::new(),
                                    expected_fence: None,
                                    history_status: None,
                                    wallet: *wallet,
                                    balances: Vec::new(),
                                    activity_cutoff_unix: NOW - 60,
                                    anchored_at_unix: NOW - 60,
                                    ledger_hash_after: "test-ledger".to_owned(),
                                    positions_proof_hash: "test-proof".to_owned(),
                                    activity_bounds_json: "{}".to_owned(),
                                    source_log_generation: "test-gen".to_owned(),
                                    proof_json: "{}".to_owned(),
                                    recorded_at_unix: NOW - 60,
                                })
                                .collect();
                            fake_paper_state.install_anchors(&installs).unwrap();
                            if failure == Some("artifact")
                                || (failure == Some("proof_mixed")
                                    && control_log
                                        .lock()
                                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                                        .is_empty())
                            {
                                let conn = rusqlite::Connection::open(&state_path).unwrap();
                                conn.execute("DELETE FROM position_validations", [])
                                    .unwrap();
                            }
                            if failure == Some("proof_mixed")
                                && !control_log
                                    .lock()
                                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                                    .is_empty()
                            {
                                rusqlite::Connection::open(&state_path)
                                    .unwrap()
                                    .execute("UPDATE wallet_history_status_v2 SET proof_json = CAST(X'FF' AS TEXT) WHERE wallet_hex = ?1", [wallets[0].to_string()])
                                    .unwrap();
                            }
                            control_log
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner)
                                .push((wallets.into_iter().collect(), members(&control_live)));
                            acknowledged.send(()).unwrap();
                        }
                        OrchestratorControl::CommitActivityBucket {
                            aggregates,
                            context,
                            committed,
                        } => {
                            let result = engine
                                .commit(
                                    aggregates,
                                    context.as_ref(),
                                    crate::bucket_commit::FrozenDecisionBasis {
                                        win_rate_p: pe_core_types::Probability::ZERO,
                                        bankroll: Decimal::ZERO,
                                    },
                                )
                                .map_err(|error| error.to_string());
                            committed.send(result).unwrap();
                        }
                        OrchestratorControl::InstallAnchors {
                            installs,
                            acknowledged,
                        } => {
                            acknowledged
                                .send(engine.install_anchors(&installs))
                                .unwrap();
                        }
                        OrchestratorControl::CaptureAdmissionLedger { wallet, captured } => {
                            captured
                                .send(
                                    crate::position_seeder::ledger_capture(
                                        engine.ledger(),
                                        &fake_paper_state,
                                        wallet,
                                    )
                                    .map_err(|error| error.to_string()),
                                )
                                .unwrap();
                        }
                        OrchestratorControl::PublishMembership {
                            change,
                            replacements,
                            checks,
                            acknowledged,
                        } => {
                            let publication_attempt =
                                publication_count.fetch_add(1, Ordering::SeqCst);
                            publication_batches
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner)
                                .push(change.ranking_batch_id);
                            if failure == Some("locked_fence")
                                && publication_attempt == 0
                                && let Some(wallet) = change.added.first()
                            {
                                rusqlite::Connection::open(&state_path)
                                    .unwrap()
                                    .execute(
                                        "INSERT INTO wallet_fences (wallet_hex, source_trade_id, cause, proof_json, fenced_at_unix) VALUES (?1, 'test', 'invalid_mapping', '{}', 1)",
                                        [wallet.to_string()],
                                    )
                                    .unwrap();
                            }
                            if failure == Some("locked_history")
                                && publication_attempt == 0
                                && let Some(wallet) = change.added.first()
                            {
                                rusqlite::Connection::open(&state_path)
                                    .unwrap()
                                    .execute(
                                        "UPDATE wallet_history_status_v2 SET complete = 0 WHERE wallet_hex = ?1",
                                        [wallet.to_string()],
                                    )
                                    .unwrap();
                            }
                            if failure == Some("proof_twice")
                                && let Some(wallet) = change.added.first()
                            {
                                rusqlite::Connection::open(&state_path).unwrap()
                                    .execute("UPDATE position_validations SET recorded_at_unix = recorded_at_unix + 1 WHERE wallet_hex = ?1", [wallet.to_string()]).unwrap();
                            }
                            if failure == Some("locked_knockout") && publication_attempt == 0 {
                                let incumbent =
                                    *control_live.structural_membership().iter().next().unwrap();
                                control_live.commit_structural_change(&[incumbent], &[]);
                                control_live.replace(
                                    &HashSet::from([incumbent]),
                                    &[],
                                    change.capacity,
                                );
                            }
                            if let Err(error) = checks.recheck_and_seed(
                                &fake_paper_state,
                                &control_live,
                                &change,
                                &replacements,
                            ) {
                                acknowledged.send(Err(error.into_publish())).unwrap();
                                continue;
                            }

                            if failure == Some("uncertain_append") {
                                acknowledged
                                    .send(Err(PublishError::UncertainAppend(
                                        "injected synchronized append failure".to_owned(),
                                    )))
                                    .unwrap();
                                continue;
                            }

                            if (failure == Some("structural")
                                || (failure == Some("structural_after_first")
                                    && publication_attempt > 0))
                                && change.reason == MembershipReason::FullRerank
                            {
                                acknowledged
                                    .send(Err(PublishError::Shared(
                                        "injected replacement publication failure".to_owned(),
                                    )))
                                    .unwrap();
                                continue;
                            }
                            if let Err(error) =
                                crate::qualification::verify_published_membership_change(
                                    &change.clone().into_record(),
                                    &verifier_source_log,
                                    &control_live.structural_membership(),
                                )
                            {
                                acknowledged
                                    .send(Err(PublishError::Shared(error.to_string())))
                                    .unwrap();
                                continue;
                            }
                            let removed = change.removed.iter().copied().collect::<HashSet<_>>();
                            let additions = replacements
                                .iter()
                                .filter(|entry| change.added.contains(&entry.wallet))
                                .cloned()
                                .collect::<Vec<_>>();
                            control_live.commit_structural_change(&change.removed, &change.added);
                            control_live.replace(&removed, &additions, change.capacity);
                            apply_live_reentries(
                                &control_live,
                                &fake_paper_state,
                                &checks.reentries,
                                &replacements,
                                change.capacity,
                            );
                            checks.commit_capacity();
                            acknowledged
                                .send(Ok(pe_event_log::AppendReceipt {
                                    sequence: pe_core_types::EventSeq(1),
                                    this_hash: blake3::hash(b"test-membership"),
                                }))
                                .unwrap();
                        }
                        OrchestratorControl::ActivityFrameDecision { .. }
                        | OrchestratorControl::RetireWallet { .. }
                        | OrchestratorControl::ReconciliationUpdate { .. }
                        | OrchestratorControl::ResolutionCandidate { .. }
                        | OrchestratorControl::RiskHaltChange { .. }
                        | OrchestratorControl::DailyBoundary { .. }
                        | OrchestratorControl::SealCheck { .. } => {
                            panic!("maintenance sent an unrelated financial control")
                        }
                    }
                }
            }));
            let preparer = crate::watchlist_admission::AdmissionPreparer::new(
                control_tx.clone(),
                Arc::clone(&paper_state),
            )
            .with_source_log(source_handle.clone());
            Harness {
                live,
                projection_rx,
                paper_state,
                preparer,
                control_tx,
                source_handle,
                applied: AppliedWatchlistCapacity::new(CAP),
                writer_lock: Mutex::new(()),
                client: reqwest::Client::new(),
                base_url,
                controls,
                membership_publications,
                published_batches,
                _source_task: source_task,
                _source_triggers: source_triggers,
                _temp: temp,
            }
        }

        impl Harness {
            async fn tick_with_preparer(
                &self,
                preparer: &crate::watchlist_admission::AdmissionPreparer,
                mode: MembershipMode,
                evicted: &mut HashSet<WalletAddress>,
                marker: &mut Option<i64>,
            ) {
                let mut sync = BatchSync {
                    parking_batch: None,
                    reentries_first: true,
                    attempted_batch_id: None,
                    cooldowns: HashMap::new(),
                    started: 0,
                    accepted: 0,
                    deferred: 0,
                    unstarted: 0,
                    marker: *marker,
                    capacity_generation: self.applied.load().generation,
                    knockout_deferred: HashSet::new(),
                };
                self.tick_synced_with_preparer(preparer, mode, evicted, &mut sync)
                    .await;
                *marker = sync.marker;
            }

            async fn tick_synced_with_preparer(
                &self,
                preparer: &crate::watchlist_admission::AdmissionPreparer,
                mode: MembershipMode,
                evicted: &mut HashSet<WalletAddress>,
                sync: &mut BatchSync,
            ) {
                let cfg = MaintenanceConfig {
                    membership_mode: mode,
                    ..cfg()
                };
                maintenance_tick(
                    &self.live,
                    &self.paper_state,
                    &self.client,
                    &self.base_url,
                    "anon",
                    "",
                    &self.writer_lock,
                    &self.applied,
                    preparer,
                    &cfg,
                    self.applied.load(),
                    evicted,
                    sync,
                    NOW,
                )
                .await;
            }

            async fn tick(
                &self,
                mode: MembershipMode,
                evicted: &mut HashSet<WalletAddress>,
                marker: &mut Option<i64>,
            ) {
                let mut sync = BatchSync {
                    parking_batch: None,
                    reentries_first: true,
                    attempted_batch_id: None,
                    cooldowns: HashMap::new(),
                    started: 0,
                    accepted: 0,
                    deferred: 0,
                    unstarted: 0,
                    marker: *marker,
                    capacity_generation: self.applied.load().generation,
                    knockout_deferred: HashSet::new(),
                };
                self.tick_synced(mode, evicted, &mut sync).await;
                *marker = sync.marker;
            }

            async fn tick_synced(
                &self,
                mode: MembershipMode,
                evicted: &mut HashSet<WalletAddress>,
                sync: &mut BatchSync,
            ) {
                let cfg = MaintenanceConfig {
                    membership_mode: mode,
                    ..cfg()
                };
                maintenance_tick(
                    &self.live,
                    &self.paper_state,
                    &self.client,
                    &self.base_url,
                    "anon",
                    "",
                    &self.writer_lock,
                    &self.applied,
                    &self.preparer,
                    &cfg,
                    self.applied.load(),
                    evicted,
                    sync,
                    NOW,
                )
                .await;
            }

            fn controls(&self) -> ControlLog {
                self.controls
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone()
            }
        }

        struct FailingPositionFetcher {
            missing_mapping: Option<WalletAddress>,
            shared: HashSet<WalletAddress>,
        }

        impl PageFetcher for FailingPositionFetcher {
            async fn fetch_page(&self, url: &str) -> Result<Vec<u8>, SourceError> {
                if self
                    .shared
                    .iter()
                    .any(|wallet| url.contains(&wallet.to_string()))
                {
                    return Err(SourceError::Transient {
                        message: "exhausted venue fixture".to_owned(),
                    });
                }
                if url.contains("/activity?") {
                    return Ok(b"[]".to_vec());
                }
                if url.contains("/positions?") {
                    if self
                        .missing_mapping
                        .is_some_and(|wallet| url.contains(&wallet.to_string()))
                    {
                        return serde_json::to_vec(&vec![serde_json::json!({
                            "proxyWallet": self.missing_mapping.unwrap(),
                            "asset": "asset-unmapped",
                            "conditionId": format!("0x{}", "a".repeat(40)),
                            "size": "1",
                            "outcomeIndex": 0
                        })])
                        .map_err(|error| SourceError::Fatal {
                            message: error.to_string(),
                        });
                    }
                    return Ok(b"[]".to_vec());
                }
                Err(SourceError::Fatal {
                    message: format!("unexpected fixture URL: {url}"),
                })
            }
        }

        fn validator_preparer(
            h: &Harness,
            missing_mapping: Option<WalletAddress>,
            shared: HashSet<WalletAddress>,
        ) -> crate::watchlist_admission::AdmissionPreparer {
            let fetcher: Arc<dyn ReconciliationFetcher> = Arc::new(FailingPositionFetcher {
                missing_mapping,
                shared,
            });
            let identity_sink = Arc::new(tokio::sync::Mutex::new(
                SourceEventSink::open(h._temp.path().join("identity-source.log")).unwrap(),
            ));
            let identity = Arc::new(AssetIdentityResolver::new(
                Arc::clone(&fetcher),
                "https://example.test".to_owned(),
                GAMMA_BATCH_SIZE,
                identity_sink,
            ));
            let validator = CausalPositionValidator::new(
                fetcher,
                "https://example.test",
                "maintenance-validation-test",
                identity,
            )
            .with_clock(Arc::new(|| NOW));
            crate::watchlist_admission::AdmissionPreparer::with_validator(
                h.control_tx.clone(),
                Arc::clone(&h.paper_state),
                validator,
            )
            .with_source_log(h.source_handle.clone())
        }

        fn set(wallets: &[WalletAddress]) -> HashSet<WalletAddress> {
            wallets.iter().copied().collect()
        }

        /// PASS: given the durable A/N generation left by a crash before MembershipChanged, the
        /// first tick prepares and applies published B; FAIL: marker N is replaced by B at boot
        /// and the tick suppresses the pending transition.
        #[tokio::test]
        async fn crash_before_membership_record_first_tick_applies_new_batch() {
            let (a, b) = (wallet(1), wallet(2));
            let mut fake = Fake::new(Some(2));
            fake.ranking_entries = vec![row(2, 1, a), row(2, 2, b)];
            let h = harness(fake, &[a]).await;
            let (mut evicted, mut marker) = (HashSet::new(), Some(1));

            h.tick(MembershipMode::FullRerank, &mut evicted, &mut marker)
                .await;

            // Only the newcomer was prepared, and membership was still the old set when the
            // orchestrator applied its maps.
            assert_eq!(h.controls(), vec![(set(&[b]), set(&[a]))]);
            assert_eq!(members(&h.live), set(&[a, b]));
            assert_eq!(marker, Some(2));
            assert_eq!(h.paper_state.cursor(&b).unwrap(), Some(NOW - 60));
        }

        #[tokio::test]
        async fn full_rerank_wallet_deferral_applies_ranked_removal() {
            let (a, b) = (wallet(1), wallet(2));
            let mut fake = Fake::new(Some(2));
            fake.ranking_entries = vec![row(2, 1, b)];
            fake.history_ok = false;
            let position_hits = Arc::clone(&fake.position_hits);
            let h = harness(fake, &[a]).await;
            let (mut evicted, mut marker) = (HashSet::new(), Some(1));

            h.tick(MembershipMode::FullRerank, &mut evicted, &mut marker)
                .await;

            assert!(h.controls().is_empty());
            assert!(
                members(&h.live).is_empty(),
                "ranked-out wallet was retained"
            );
            assert_eq!(marker, Some(2), "published removal did not advance marker");
            assert_eq!(position_hits.load(Ordering::SeqCst), 0);
            assert_eq!(h.paper_state.cursor(&b).unwrap(), None);
            let audits = pe_event_log::Reader::replay(h._temp.path().join("source.log"))
                .unwrap()
                .filter_map(Result::ok)
                .filter(|(_, frame)| {
                    frame.source_id.0 == crate::watchlist_admission::MEMBERSHIP_DEFERRAL_SOURCE_ID
                })
                .map(|(_, frame)| {
                    serde_json::from_slice::<serde_json::Value>(&frame.payload).unwrap()
                })
                .collect::<Vec<_>>();
            assert_eq!(audits.len(), 1);
            assert_eq!(audits[0]["deferrals"][0]["wallet"], b.to_string());
            assert_eq!(audits[0]["outcome"]["type"], "published");
        }

        #[tokio::test]
        async fn full_rerank_deferral_fills_cap_from_next_survivor_and_records_audit() {
            let (old, blocked, first, second, third) =
                (wallet(41), wallet(42), wallet(43), wallet(44), wallet(45));
            let mut fake = Fake::new(Some(2));
            fake.ranking_entries = vec![
                row(2, 1, blocked),
                row(2, 2, first),
                row(2, 3, second),
                row(2, 4, third),
            ];
            fake.history_missing.insert(blocked);
            let h = harness(fake, &[old]).await;
            let (mut evicted, mut marker) = (HashSet::new(), Some(1));
            h.tick(MembershipMode::FullRerank, &mut evicted, &mut marker)
                .await;
            assert_eq!(marker, Some(2));
            assert_eq!(members(&h.live), set(&[first, second, third]));
            assert_eq!(
                h.controls()
                    .iter()
                    .map(|(wallets, _)| wallets.clone())
                    .collect::<Vec<_>>(),
                vec![set(&[first]), set(&[second]), set(&[third])]
            );
            assert_eq!(h.membership_publications.load(Ordering::SeqCst), 1);
            let audits = pe_event_log::Reader::replay(h._temp.path().join("source.log"))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
                .into_iter()
                .filter(|(_, frame)| {
                    frame.source_id.0 == crate::watchlist_admission::MEMBERSHIP_DEFERRAL_SOURCE_ID
                })
                .map(|(_, frame)| {
                    serde_json::from_slice::<serde_json::Value>(&frame.payload).unwrap()
                })
                .collect::<Vec<_>>();
            assert_eq!(audits.len(), 1);
            assert_eq!(audits[0]["deferrals"][0]["kind"], "history.missing");
            assert_eq!(audits[0]["deferrals"][0]["wallet"], blocked.to_string());
            assert_eq!(audits[0]["outcome"]["type"], "published");
        }

        #[tokio::test]
        async fn successive_deferrals_near_cap_prepare_carried_wallet_once() {
            let (incumbent, first, blocked_a, blocked_b, second, third) = (
                wallet(89),
                wallet(90),
                wallet(91),
                wallet(92),
                wallet(93),
                wallet(94),
            );
            let mut fake = Fake::new(Some(2));
            fake.ranking_entries = vec![
                row(2, 1, first),
                row(2, 2, blocked_a),
                row(2, 3, blocked_b),
                row(2, 4, second),
                row(2, 5, third),
            ];
            fake.history_missing.extend([blocked_a, blocked_b]);
            let h = harness(fake, &[incumbent]).await;
            let (mut evicted, mut marker) = (HashSet::new(), Some(1));

            h.tick(MembershipMode::FullRerank, &mut evicted, &mut marker)
                .await;
            assert_eq!(marker, Some(2));
            assert_eq!(members(&h.live), set(&[first, second, third]));
            let prepared = h.controls();
            assert_eq!(
                prepared
                    .iter()
                    .map(|(wallets, _)| wallets.clone())
                    .collect::<Vec<_>>(),
                vec![set(&[first]), set(&[second]), set(&[third])]
            );
            let audits = pe_event_log::Reader::replay(h._temp.path().join("source.log"))
                .unwrap()
                .filter_map(Result::ok)
                .filter(|(_, frame)| {
                    frame.source_id.0 == crate::watchlist_admission::MEMBERSHIP_DEFERRAL_SOURCE_ID
                })
                .map(|(_, frame)| {
                    serde_json::from_slice::<serde_json::Value>(&frame.payload).unwrap()
                })
                .collect::<Vec<_>>();
            assert_eq!(audits.len(), 1);
            assert_eq!(audits[0]["deferrals"].as_array().unwrap().len(), 2);
        }

        #[tokio::test]
        async fn next_ranking_batch_retries_a_previously_deferred_wallet() {
            let (old, deferred, survivor) = (wallet(46), wallet(47), wallet(48));
            let mut fake = Fake::new(Some(2));
            fake.ranking_entries = vec![
                row(2, 1, deferred),
                row(2, 2, survivor),
                row(3, 1, deferred),
                row(3, 2, survivor),
            ];
            fake.history_missing.insert(deferred);
            let latest_batch = Arc::clone(&fake.latest_batch);
            let h = harness(fake, &[old]).await;
            let (mut evicted, mut marker) = (HashSet::new(), Some(1));

            h.tick(MembershipMode::FullRerank, &mut evicted, &mut marker)
                .await;
            assert_eq!(marker, Some(2));
            assert_eq!(members(&h.live), set(&[survivor]));
            assert_eq!(h.controls().len(), 1);

            h.paper_state
                .record_reconciled_history_status(&pe_paper_state::WalletHistoryStatusRecord {
                    wallet: deferred,
                    complete: true,
                    proof_json: "{}".to_owned(),
                    updated_at_unix: NOW,
                })
                .unwrap();
            latest_batch.store(3, Ordering::SeqCst);
            h.tick(MembershipMode::FullRerank, &mut evicted, &mut marker)
                .await;
            assert_eq!(marker, Some(3));
            assert_eq!(members(&h.live), set(&[deferred, survivor]));
            assert_eq!(
                h.controls()
                    .iter()
                    .map(|(wallets, _)| wallets.clone())
                    .collect::<Vec<_>>(),
                vec![set(&[survivor]), set(&[deferred])]
            );
        }

        #[tokio::test]
        async fn knockout_deferral_uses_next_ranked_candidate_for_freed_slot() {
            let (idle, kept_a, kept_b, blocked, replacement) =
                (wallet(51), wallet(52), wallet(53), wallet(54), wallet(55));
            let mut fake = Fake::new(Some(1));
            fake.ranking_entries = vec![row(1, 1, blocked), row(1, 2, replacement)];
            fake.history_missing.insert(blocked);
            let h = harness(fake, &[idle, kept_a, kept_b]).await;
            h.paper_state.set_cursor(&idle, NOW - 300_000).unwrap();
            h.paper_state.set_cursor(&kept_a, NOW - 1).unwrap();
            h.paper_state.set_cursor(&kept_b, NOW - 1).unwrap();
            let mut evicted = HashSet::new();
            let mut sync = BatchSync {
                parking_batch: None,
                reentries_first: true,
                attempted_batch_id: None,
                cooldowns: HashMap::new(),
                started: 0,
                accepted: 0,
                deferred: 0,
                unstarted: 0,
                marker: Some(1),
                capacity_generation: h.applied.load().generation,
                knockout_deferred: HashSet::new(),
            };
            h.tick_synced(MembershipMode::Knockout, &mut evicted, &mut sync)
                .await;
            assert_eq!(members(&h.live), set(&[kept_a, kept_b, replacement]));
            assert_eq!(h.paper_state.cursor(&replacement).unwrap(), Some(NOW - 60));
            assert_eq!(h.paper_state.cursor(&blocked).unwrap(), None);
            assert!(evicted.contains(&idle));
            assert!(sync.knockout_deferred.contains(&blocked));
            assert_eq!(
                h.controls()
                    .iter()
                    .map(|(wallets, _)| wallets.clone())
                    .collect::<Vec<_>>(),
                vec![set(&[replacement])]
            );
        }

        #[tokio::test]
        async fn knockout_mapping_deferral_uses_next_candidate_once_per_batch() {
            let (idle, kept_a, kept_b, blocked, replacement) =
                (wallet(66), wallet(67), wallet(68), wallet(69), wallet(70));
            let mut fake = Fake::new(Some(1));
            fake.ranking_entries = vec![row(1, 1, blocked), row(1, 2, replacement)];
            let h = harness(fake, &[idle, kept_a, kept_b]).await;
            h.paper_state.set_cursor(&idle, NOW - 300_000).unwrap();
            let preparer = validator_preparer(&h, Some(blocked), HashSet::new());
            let mut evicted = HashSet::new();
            let mut sync = BatchSync {
                parking_batch: None,
                reentries_first: true,
                attempted_batch_id: None,
                cooldowns: HashMap::new(),
                started: 0,
                accepted: 0,
                deferred: 0,
                unstarted: 0,
                marker: Some(1),
                capacity_generation: h.applied.load().generation,
                knockout_deferred: HashSet::new(),
            };

            h.tick_synced_with_preparer(
                &preparer,
                MembershipMode::Knockout,
                &mut evicted,
                &mut sync,
            )
            .await;
            assert_eq!(members(&h.live), set(&[kept_a, kept_b, replacement]));
            assert_eq!(h.paper_state.cursor(&replacement).unwrap(), Some(NOW - 60));
            assert!(evicted.contains(&idle));
            assert!(sync.knockout_deferred.contains(&blocked));
            assert_eq!(h.membership_publications.load(Ordering::SeqCst), 1);
            let audits = || {
                pe_event_log::Reader::replay(h._temp.path().join("source.log"))
                    .unwrap()
                    .filter_map(Result::ok)
                    .filter(|(_, frame)| {
                        frame.source_id.0
                            == crate::watchlist_admission::MEMBERSHIP_DEFERRAL_SOURCE_ID
                    })
                    .map(|(_, frame)| {
                        serde_json::from_slice::<serde_json::Value>(&frame.payload).unwrap()
                    })
                    .collect::<Vec<_>>()
            };
            assert_eq!(audits().len(), 1);
            assert_eq!(
                audits()[0]["deferrals"][0]["kind"],
                "positions.missing_activity_mapping"
            );

            h.tick_synced_with_preparer(
                &preparer,
                MembershipMode::Knockout,
                &mut evicted,
                &mut sync,
            )
            .await;
            assert_eq!(members(&h.live), set(&[kept_a, kept_b, replacement]));
            assert_eq!(h.membership_publications.load(Ordering::SeqCst), 1);
            assert_eq!(
                audits().len(),
                1,
                "deferred wallet was retried in the same batch"
            );
        }

        #[tokio::test]
        async fn locked_wallet_rejection_replans_full_rerank_and_knockout() {
            let (incumbent, blocked, replacement) = (wallet(61), wallet(62), wallet(63));
            for failure in ["locked_fence", "locked_history"] {
                for mode in [MembershipMode::FullRerank, MembershipMode::Knockout] {
                    let batch = if mode == MembershipMode::FullRerank {
                        2
                    } else {
                        1
                    };
                    let mut fake = Fake::new(Some(batch));
                    fake.ranking_entries = vec![row(batch, 1, blocked), row(batch, 2, replacement)];
                    fake.failure = Some(failure);
                    let h = harness(fake, &[incumbent]).await;
                    if mode == MembershipMode::Knockout {
                        h.paper_state.set_cursor(&incumbent, NOW - 300_000).unwrap();
                    }
                    let (mut evicted, mut marker) = (HashSet::new(), Some(1));
                    h.tick(mode, &mut evicted, &mut marker).await;
                    assert_eq!(members(&h.live), set(&[replacement]));
                    assert_eq!(h.membership_publications.load(Ordering::SeqCst), 2);
                    if failure == "locked_fence" {
                        assert!(h.paper_state.is_wallet_fenced(&blocked).unwrap());
                    } else {
                        assert!(!h.paper_state.wallet_history_complete(&blocked).unwrap());
                    }
                    assert_eq!(marker, Some(batch));
                }
            }
        }

        #[tokio::test]
        async fn locked_knockout_shape_stales_rerank_and_next_tick_retries_batch() {
            let (incumbent, candidate) = (wallet(84), wallet(85));
            let mut fake = Fake::new(Some(2));
            fake.ranking_entries = vec![row(2, 1, candidate)];
            fake.failure = Some("locked_knockout");
            let h = harness(fake, &[incumbent]).await;
            let (mut evicted, mut marker) = (HashSet::new(), Some(1));

            h.tick(MembershipMode::FullRerank, &mut evicted, &mut marker)
                .await;
            assert_eq!(marker, Some(1), "stale attempt advanced the batch marker");
            assert!(h.live.structural_membership().is_empty());
            assert_eq!(h.membership_publications.load(Ordering::SeqCst), 1);

            h.tick(MembershipMode::FullRerank, &mut evicted, &mut marker)
                .await;
            assert_eq!(marker, Some(2));
            assert_eq!(members(&h.live), set(&[candidate]));
            assert_eq!(h.membership_publications.load(Ordering::SeqCst), 2);
            assert_eq!(
                h.published_batches
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .as_slice(),
                &[Some(2), Some(2)]
            );
        }

        #[tokio::test]
        async fn uncertain_paper_append_requests_no_deferral_audit_from_either_caller() {
            let (incumbent, candidate) = (wallet(87), wallet(88));
            for mode in [MembershipMode::FullRerank, MembershipMode::Knockout] {
                let batch = if mode == MembershipMode::FullRerank {
                    2
                } else {
                    1
                };
                let mut fake = Fake::new(Some(batch));
                fake.ranking_entries = vec![row(batch, 1, candidate)];
                fake.failure = Some("uncertain_append");
                let h = harness(fake, &[incumbent]).await;
                if mode == MembershipMode::Knockout {
                    h.paper_state.set_cursor(&incumbent, NOW - 300_000).unwrap();
                }
                let (mut evicted, mut marker) = (HashSet::new(), Some(1));
                h.tick(mode, &mut evicted, &mut marker).await;
                assert_eq!(marker, Some(1));
                assert_eq!(members(&h.live), set(&[incumbent]));
                assert_eq!(h.membership_publications.load(Ordering::SeqCst), 1);
                assert_eq!(
                    pe_event_log::Reader::replay(h._temp.path().join("source.log"))
                        .unwrap()
                        .filter_map(Result::ok)
                        .filter(|(_, frame)| {
                            frame.source_id.0
                                == crate::watchlist_admission::MEMBERSHIP_DEFERRAL_SOURCE_ID
                        })
                        .count(),
                    0,
                    "{mode:?} requested an audit after an uncertain paper append"
                );
            }
        }

        #[tokio::test]
        async fn shared_preparation_preserves_rerank_but_knockout_publishes_eviction_only() {
            let (incumbent, replacement) = (wallet(56), wallet(57));
            let mut rerank_fake = Fake::new(Some(2));
            rerank_fake.ranking_entries = vec![row(2, 1, replacement)];
            rerank_fake.failure = Some("shared_prepare");
            let rerank = harness(rerank_fake, &[incumbent]).await;
            let (mut evicted, mut marker) = (HashSet::new(), Some(1));
            rerank
                .tick(MembershipMode::FullRerank, &mut evicted, &mut marker)
                .await;
            assert_eq!(members(&rerank.live), set(&[incumbent]));
            assert_eq!(marker, Some(1));
            assert_eq!(rerank.membership_publications.load(Ordering::SeqCst), 0);
            assert_eq!(
                pe_event_log::Reader::replay(rerank._temp.path().join("source.log"))
                    .unwrap()
                    .filter_map(Result::ok)
                    .filter(|(_, frame)| {
                        frame.source_id.0
                            == crate::watchlist_admission::MEMBERSHIP_DEFERRAL_SOURCE_ID
                    })
                    .count(),
                0
            );

            let mut knockout_fake = Fake::new(Some(1));
            knockout_fake.ranking_entries = vec![row(1, 1, replacement)];
            knockout_fake.failure = Some("shared_prepare");
            let knockout = harness(knockout_fake, &[incumbent]).await;
            knockout
                .paper_state
                .set_cursor(&incumbent, NOW - 300_000)
                .unwrap();
            let (mut evicted, mut marker) = (HashSet::new(), Some(1));
            knockout
                .tick(MembershipMode::Knockout, &mut evicted, &mut marker)
                .await;
            assert!(members(&knockout.live).is_empty());
            assert!(evicted.contains(&incumbent));
            let audit_count =
                pe_event_log::Reader::replay(knockout._temp.path().join("source.log"))
                    .unwrap()
                    .filter_map(Result::ok)
                    .filter(|(_, frame)| {
                        frame.source_id.0
                            == crate::watchlist_admission::MEMBERSHIP_DEFERRAL_SOURCE_ID
                    })
                    .count();
            assert_eq!(audit_count, 0);
        }

        #[tokio::test]
        async fn shared_live_reentry_retries_after_applied_batch() {
            let retained = wallet(58);
            for mode in [MembershipMode::FullRerank, MembershipMode::Knockout] {
                let mut fake = Fake::new(Some(2));
                fake.ranking_entries = vec![row(2, 1, retained)];
                fake.failure = Some("shared_prepare");
                let h = harness(fake, &[retained]).await;
                h.live.remove_fenced(&set(&[retained]));
                let (mut evicted, mut marker) = (HashSet::new(), Some(1));
                h.tick(mode, &mut evicted, &mut marker).await;
                assert_eq!(marker, Some(2));
                assert_eq!(h.live.structural_membership(), set(&[retained]));
                assert_eq!(h.membership_publications.load(Ordering::SeqCst), 0);
            }
        }

        #[tokio::test]
        async fn second_locked_proof_change_defers_wallet_without_repreparing() {
            let (incumbent, candidate) = (wallet(59), wallet(60));
            let mut fake = Fake::new(Some(2));
            fake.ranking_entries = vec![row(2, 1, candidate)];
            fake.failure = Some("proof_twice");
            let h = harness(fake, &[incumbent]).await;
            let (mut evicted, mut marker) = (HashSet::new(), Some(1));
            h.tick(MembershipMode::FullRerank, &mut evicted, &mut marker)
                .await;
            assert_eq!(marker, Some(2));
            assert!(members(&h.live).is_empty());
            assert_eq!(
                h.controls().len(),
                1,
                "carried-forward wallet was prepared twice"
            );
            assert_eq!(h.membership_publications.load(Ordering::SeqCst), 3);
            let audits = pe_event_log::Reader::replay(h._temp.path().join("source.log"))
                .unwrap()
                .filter_map(Result::ok)
                .filter(|(_, frame)| {
                    frame.source_id.0 == crate::watchlist_admission::MEMBERSHIP_DEFERRAL_SOURCE_ID
                })
                .map(|(_, frame)| {
                    serde_json::from_slice::<serde_json::Value>(&frame.payload).unwrap()
                })
                .collect::<Vec<_>>();
            assert_eq!(audits.len(), 1);
            assert_eq!(
                audits[0]["deferrals"][0]["kind"],
                "publication.proof_changed"
            );
        }

        #[tokio::test]
        async fn full_rerank_zero_survivor_batch_empties_membership_with_no_requests() {
            let a = wallet(1);
            let fake = Fake::new(Some(2));
            let (activity_hits, position_hits) = (
                Arc::clone(&fake.activity_hits),
                Arc::clone(&fake.position_hits),
            );
            let h = harness(fake, &[a]).await;
            let (mut evicted, mut marker) = (HashSet::new(), Some(1));

            h.tick(MembershipMode::FullRerank, &mut evicted, &mut marker)
                .await;

            assert!(h.controls().is_empty());
            assert!(members(&h.live).is_empty());
            assert_eq!(marker, Some(2));
            assert_eq!(activity_hits.load(Ordering::SeqCst), 0);
            assert_eq!(position_hits.load(Ordering::SeqCst), 0);
        }

        #[tokio::test]
        async fn filtered_noop_is_retried_after_restart_without_a_structural_record() {
            let retained = wallet(64);
            let mut fake = Fake::new(Some(2));
            fake.ranking_entries = vec![row(2, 1, retained)];
            let ranking_hits = Arc::clone(&fake.pinned_ranking_hits);
            let h = harness(fake, &[retained]).await;
            let (mut evicted, mut marker) = (HashSet::new(), Some(1));

            h.tick(MembershipMode::FullRerank, &mut evicted, &mut marker)
                .await;
            assert_eq!(marker, Some(2));
            assert_eq!(ranking_hits.load(Ordering::SeqCst), 1);
            assert_eq!(h.membership_publications.load(Ordering::SeqCst), 0);
            assert_eq!(members(&h.live), set(&[retained]));

            // Boot reconstructs the last recorded marker, not this process's no-op marker.
            marker = Some(1);
            h.tick(MembershipMode::FullRerank, &mut evicted, &mut marker)
                .await;
            assert_eq!(marker, Some(2));
            assert_eq!(ranking_hits.load(Ordering::SeqCst), 2);
            assert_eq!(h.membership_publications.load(Ordering::SeqCst), 0);
            assert_eq!(members(&h.live), set(&[retained]));
        }

        #[tokio::test]
        async fn knockout_after_filtered_noop_persists_batch_marker_for_restart() {
            let retained = wallet(65);
            let mut fake = Fake::new(Some(2));
            fake.ranking_entries = vec![row(2, 1, retained)];
            let ranking_hits = Arc::clone(&fake.pinned_ranking_hits);
            let h = harness(fake, &[retained]).await;
            let (mut evicted, mut marker) = (HashSet::new(), Some(1));

            h.tick(MembershipMode::FullRerank, &mut evicted, &mut marker)
                .await;
            assert_eq!(marker, Some(2));
            assert_eq!(h.membership_publications.load(Ordering::SeqCst), 0);
            h.paper_state.set_cursor(&retained, NOW - 300_000).unwrap();
            h.tick(MembershipMode::Knockout, &mut evicted, &mut marker)
                .await;
            assert!(members(&h.live).is_empty());
            assert_eq!(
                *h.published_batches
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner),
                vec![Some(2)]
            );

            let before_restart = ranking_hits.load(Ordering::SeqCst);
            marker = Some(2); // The knockout record is the boot-replayed marker.
            h.tick(MembershipMode::FullRerank, &mut evicted, &mut marker)
                .await;
            assert_eq!(ranking_hits.load(Ordering::SeqCst), before_restart);
            assert_eq!(h.membership_publications.load(Ordering::SeqCst), 1);
            assert_eq!(marker, Some(2));
        }

        #[tokio::test]
        async fn full_rerank_absent_marker_applies_the_pinned_batch() {
            // A failed boot batch read used to stamp the marker without applying its batch;
            // the first tick now performs the ordinary pinned, prepared apply.
            let (a, b) = (wallet(1), wallet(2));
            let mut fake = Fake::new(Some(5));
            fake.ranking_entries = vec![row(5, 1, b)];
            let h = harness(fake, &[a]).await;
            let (mut evicted, mut marker) = (HashSet::new(), None);

            h.tick(MembershipMode::FullRerank, &mut evicted, &mut marker)
                .await;

            assert_eq!(h.controls(), vec![(set(&[b]), set(&[a]))]);
            assert_eq!(members(&h.live), set(&[b]));
            assert_eq!(marker, Some(5));
        }

        #[tokio::test]
        async fn full_rerank_applies_the_triggering_batch_not_a_newer_one() {
            // Batch 3 is published between the trigger read (batch 2) and the row read. The
            // moving `latest_ranking` view would serve batch 3; the pinned read serves batch 2
            // and the marker names the batch whose rows were applied.
            let (a, b, c) = (wallet(1), wallet(2), wallet(3));
            let mut fake = Fake::new(Some(2));
            let fenced = [wallet(4), wallet(5), wallet(6)];
            fake.ranking_entries = fenced
                .iter()
                .enumerate()
                .map(|(i, wallet)| row(2, i64::try_from(i + 1).unwrap(), *wallet))
                .collect();
            fake.ranking_entries.extend([row(2, 4, b), row(3, 1, c)]);
            fake.latest_ranking = vec![row(3, 1, c)];
            let h = harness(fake, &[a]).await;
            for wallet in fenced {
                fence(&h, wallet);
            }
            let (mut evicted, mut marker) = (HashSet::new(), Some(1));

            h.tick(MembershipMode::FullRerank, &mut evicted, &mut marker)
                .await;

            assert_eq!(h.controls(), vec![(set(&[b]), set(&[a]))]);
            assert_eq!(members(&h.live), set(&[b]));
            assert_eq!(marker, Some(2));
        }

        #[tokio::test]
        async fn knockout_batch_restores_structural_wallets_missing_from_live() {
            let (deferred, present, newer) = (wallet(1), wallet(2), wallet(3));
            for empty_after_boot in [false, true] {
                let mut fake = Fake::new(Some(2));
                fake.ranking_entries = vec![row(2, 1, deferred), row(2, 2, present)];
                // The moving view has already advanced. Reentry must use batch 2's score.
                fake.latest_ranking = vec![row(3, 1, newer)];
                let h = harness(fake, &[deferred, present]).await;
                let removed = if empty_after_boot {
                    set(&[deferred, present])
                } else {
                    set(&[deferred])
                };
                h.live.remove_fenced(&removed);
                assert_eq!(h.live.structural_membership(), set(&[deferred, present]));
                assert_eq!(members(&h.live).is_empty(), empty_after_boot);
                let (mut evicted, mut marker) = (HashSet::new(), Some(1));

                h.tick(MembershipMode::Knockout, &mut evicted, &mut marker)
                    .await;

                assert_eq!(marker, Some(2));
                assert_eq!(members(&h.live), set(&[deferred, present]));
                assert_eq!(h.live.structural_membership(), set(&[deferred, present]));
                let snapshot = h.live.snapshot();
                let restored = snapshot
                    .entries
                    .iter()
                    .find(|entry| entry.wallet == deferred)
                    .unwrap();
                assert_eq!(restored.leader_score_bps.0, 2_000);
                assert_eq!(h.membership_publications.load(Ordering::SeqCst), 0);
                assert_eq!(h.controls().len(), if empty_after_boot { 2 } else { 1 });
            }
        }

        #[tokio::test]
        async fn pinned_batch_live_reentry_runs_before_full_rerank_return() {
            let deferred = wallet(71);
            let mut fake = Fake::new(Some(2));
            fake.ranking_entries = vec![row(2, 1, deferred)];
            // A stale ranking timestamp: only the wallet's recent activity clock admits it.
            fake.ranking_entries[0]["last_trade_unix"] = serde_json::json!(NOW - 300_000);
            let pinned_reads = Arc::clone(&fake.pinned_ranking_hits);
            let h = harness(fake, &[deferred]).await;
            h.paper_state.set_cursor(&deferred, NOW - 300_000).unwrap();
            h.paper_state.set_activity(&deferred, NOW - 60).unwrap();
            h.live.remove_fenced(&set(&[deferred]));
            let mut sync = BatchSync {
                parking_batch: None,
                reentries_first: true,
                attempted_batch_id: None,
                cooldowns: HashMap::new(),
                started: 0,
                accepted: 0,
                deferred: 0,
                unstarted: 0,
                marker: Some(1),
                capacity_generation: h.applied.load().generation,
                knockout_deferred: HashSet::new(),
            };
            h.tick_synced(MembershipMode::FullRerank, &mut HashSet::new(), &mut sync)
                .await;
            assert_eq!(sync.marker, Some(2));
            assert_eq!(members(&h.live), set(&[deferred]));
            assert_eq!(h.controls().len(), 1);
            assert_eq!(
                pinned_reads.load(Ordering::SeqCst),
                1,
                "the successful rerank's pinned entries are reused for live reentry"
            );
        }

        #[tokio::test]
        async fn pinned_batch_live_reentry_precedes_edge_failure_and_full_capacity_returns() {
            // Loaded statistics and a recent clock admit both wallets in one tick. A stale clock
            // (full capacity) or hidden statistics (edge failure) leaves the stale-ranked wallet
            // out, unparked, until that evidence recovers within the same batch.
            for (full_capacity, first_activity) in
                [(true, NOW - 60), (true, NOW - 300_000), (false, NOW - 60)]
            {
                let deferred = wallet(72);
                // Stale in the pinned batch; only its own activity clock can bring it back.
                let stale_ranked = wallet(75);
                let peers = if full_capacity {
                    vec![wallet(73)]
                } else {
                    Vec::new()
                };
                let mut initial = vec![deferred, stale_ranked];
                initial.extend(peers.iter().copied());
                let mut fake = Fake::new(Some(1));
                fake.ranking_entries = initial
                    .iter()
                    .enumerate()
                    .map(|(index, wallet)| row(1, i64::try_from(index + 1).unwrap(), *wallet))
                    .collect();
                fake.ranking_entries[1]["last_trade_unix"] = serde_json::json!(NOW - 300_000);
                let pinned_reads = Arc::clone(&fake.pinned_ranking_hits);
                let h = harness(fake, &initial).await;
                h.paper_state
                    .set_cursor(&stale_ranked, NOW - 300_000)
                    .unwrap();
                h.paper_state
                    .set_activity(&stale_ranked, first_activity)
                    .unwrap();
                h.live.remove_fenced(&set(&[deferred, stale_ranked]));
                let paper_db = h._temp.path().join("paper.db");
                if !full_capacity {
                    rusqlite::Connection::open(&paper_db)
                        .unwrap()
                        .execute("ALTER TABLE fills RENAME TO hidden_fills", [])
                        .unwrap();
                }
                let mut sync = BatchSync {
                    parking_batch: None,
                    reentries_first: true,
                    attempted_batch_id: None,
                    cooldowns: HashMap::new(),
                    started: 0,
                    accepted: 0,
                    deferred: 0,
                    unstarted: 0,
                    marker: Some(1),
                    capacity_generation: h.applied.load().generation,
                    knockout_deferred: HashSet::new(),
                };
                h.tick_synced(MembershipMode::Knockout, &mut HashSet::new(), &mut sync)
                    .await;
                let at_once = full_capacity && first_activity == NOW - 60;
                let mut first_live = if at_once {
                    vec![deferred, stale_ranked]
                } else {
                    vec![deferred]
                };
                first_live.extend(peers.iter().copied());
                assert_eq!(members(&h.live), set(&first_live));
                assert_eq!(h.controls().len(), if at_once { 2 } else { 1 });
                assert!(
                    sync.knockout_deferred.is_empty(),
                    "a retried wallet is never parked"
                );
                if full_capacity {
                    h.paper_state.set_activity(&stale_ranked, NOW - 60).unwrap();
                } else {
                    rusqlite::Connection::open(&paper_db)
                        .unwrap()
                        .execute("ALTER TABLE hidden_fills RENAME TO fills", [])
                        .unwrap();
                }
                h.tick_synced(MembershipMode::Knockout, &mut HashSet::new(), &mut sync)
                    .await;
                assert_eq!(members(&h.live), set(&initial));
                assert_eq!(h.controls().len(), 2);
                // With every structural member live, the second tick reads no ranking.
                assert_eq!(
                    pinned_reads.load(Ordering::SeqCst),
                    if at_once { 1 } else { 2 }
                );
                assert_eq!(sync.marker, Some(1));
                assert!(sync.knockout_deferred.is_empty());
                assert_eq!(
                    h.paper_state.cursor(&stale_ranked).unwrap(),
                    Some(NOW - 300_000)
                );
                assert_eq!(h.membership_publications.load(Ordering::SeqCst), 0);
            }
        }

        #[tokio::test]
        async fn reentry_report_counts_once_per_tick_and_parks_only_persistent_deferrals() {
            let (ready, missing) = (wallet(75), wallet(76));
            let mut fake = Fake::new(Some(1));
            fake.ranking_entries = vec![row(1, 1, ready), row(1, 2, missing)];
            fake.history_missing.insert(missing);
            let h = harness(fake, &[ready, missing]).await;
            h.live.remove_fenced(&set(&[ready, missing]));
            let (watchlist, last_trade) = supabase_reader::fetch_batch(
                &h.client,
                &h.base_url,
                "anon",
                "",
                1,
                MAX_ACTIVE_WATCHLIST_SIZE,
            )
            .await
            .unwrap();
            let held = (1, watchlist, last_trade);
            let mut attempted = HashSet::new();
            let mut sync = BatchSync {
                parking_batch: None,
                reentries_first: true,
                attempted_batch_id: None,
                cooldowns: HashMap::new(),
                started: 0,
                accepted: 0,
                deferred: 0,
                unstarted: 0,
                marker: Some(1),
                capacity_generation: h.applied.load().generation,
                knockout_deferred: HashSet::new(),
            };
            let first = live_reentry_tick(
                &h.live,
                &h.paper_state,
                &cfg(),
                &h.client,
                &h.base_url,
                "anon",
                "",
                &h.writer_lock,
                &h.applied,
                h.applied.load(),
                &h.preparer,
                &mut sync,
                Some(&held),
                &mut attempted,
                NOW,
                None,
            )
            .await
            .unwrap();
            assert_eq!((first.before_live, first.after_live), (0, 1));
            assert_eq!(first.admitted, vec![ready]);
            assert_eq!(first.deferred.len(), 1);
            assert_eq!(first.deferred[0].wallet, missing);
            assert_eq!(
                first.deferred[0].class,
                crate::position_seeder::FailureClass::WalletPersistent
            );
            record_live_reentry(&h.preparer, &sync, MembershipMode::Knockout, Some(first)).await;
            assert_eq!(sync.knockout_deferred, set(&[missing]));
            let second = live_reentry_tick(
                &h.live,
                &h.paper_state,
                &cfg(),
                &h.client,
                &h.base_url,
                "anon",
                "",
                &h.writer_lock,
                &h.applied,
                h.applied.load(),
                &h.preparer,
                &mut sync,
                Some(&held),
                &mut attempted,
                NOW,
                None,
            )
            .await;
            // Nothing is retryable (one wallet live, one parked): no ranking read, no report.
            assert!(second.is_none());
            assert_eq!(h.controls().len(), 1);
        }

        #[tokio::test]
        async fn persistent_reentry_deferral_reopens_only_after_batch_marker_advance() {
            let deferred = wallet(77);
            let mut fake = Fake::new(Some(2));
            fake.ranking_entries = vec![row(1, 1, deferred), row(2, 1, deferred)];
            let h = harness(fake, &[deferred]).await;
            h.live.remove_fenced(&set(&[deferred]));
            let mut sync = BatchSync {
                parking_batch: None,
                reentries_first: true,
                attempted_batch_id: None,
                cooldowns: HashMap::new(),
                started: 0,
                accepted: 0,
                deferred: 0,
                unstarted: 0,
                marker: Some(1),
                capacity_generation: h.applied.load().generation,
                knockout_deferred: set(&[deferred]),
            };
            h.tick_synced(MembershipMode::Knockout, &mut HashSet::new(), &mut sync)
                .await;
            assert_eq!(sync.marker, Some(2));
            assert!(sync.knockout_deferred.is_empty());
            assert_eq!(members(&h.live), set(&[deferred]));
            assert_eq!(h.controls().len(), 1);
        }

        #[tokio::test]
        async fn knockout_batch_restores_structural_wallet_ranked_below_the_cap() {
            let (a, b, deferred) = (wallet(1), wallet(2), wallet(3));
            let (x, y, z) = (wallet(7), wallet(8), wallet(9));
            let mut fake = Fake::new(Some(2));
            // `deferred` ranks fourth, below CAP = 3; knockout membership is not the top-CAP.
            fake.ranking_entries = vec![
                row(2, 1, x),
                row(2, 2, y),
                row(2, 3, z),
                row(2, 4, deferred),
            ];
            let h = harness(fake, &[a, b, deferred]).await;
            h.live.remove_fenced(&set(&[deferred]));
            let (mut evicted, mut marker) = (HashSet::new(), Some(1));

            h.tick(MembershipMode::Knockout, &mut evicted, &mut marker)
                .await;

            assert_eq!(marker, Some(2));
            assert_eq!(members(&h.live), set(&[a, b, deferred]));
            assert_eq!(h.live.structural_membership(), set(&[a, b, deferred]));
            assert_eq!(h.membership_publications.load(Ordering::SeqCst), 0);
        }

        #[tokio::test]
        async fn knockout_batch_read_failure_keeps_marker_and_eviction_memory() {
            let (a, deferred, knocked_out) = (wallet(1), wallet(2), wallet(9));
            let mut fake = Fake::new(Some(2));
            fake.ranking_entries = vec![row(2, 1, deferred)];
            fake.entries_fail = true;
            let h = harness(fake, &[a, deferred]).await;
            h.live.remove_fenced(&set(&[deferred]));
            let (mut evicted, mut marker) = (set(&[knocked_out]), Some(1));

            h.tick(MembershipMode::Knockout, &mut evicted, &mut marker)
                .await;

            assert_eq!(
                marker,
                Some(1),
                "a failed pinned read retries the transition"
            );
            assert!(evicted.contains(&knocked_out));
            assert_eq!(members(&h.live), set(&[a]));
            assert_eq!(h.live.structural_membership(), set(&[a, deferred]));
        }

        #[tokio::test]
        async fn knockout_prepares_planned_backfill_before_publishing() {
            let (idle, bench) = (wallet(1), wallet(2));
            let mut fake = Fake::new(Some(1));
            fake.latest_ranking = vec![row(1, 1, bench)];
            let h = harness(fake, &[idle]).await;
            // Idle past the 72h threshold with no stats → inactivity eviction.
            h.paper_state.set_cursor(&idle, NOW - 300_000).unwrap();
            let (mut evicted, mut marker) = (HashSet::new(), Some(1));

            h.tick(MembershipMode::Knockout, &mut evicted, &mut marker)
                .await;

            assert_eq!(h.controls(), vec![(set(&[bench]), set(&[idle]))]);
            assert_eq!(members(&h.live), set(&[bench]));
            assert!(evicted.contains(&idle));
            assert_eq!(h.paper_state.cursor(&bench).unwrap(), Some(NOW - 60));
        }

        #[tokio::test]
        async fn knockout_preparation_failure_evicts_without_backfill() {
            let (idle, bench) = (wallet(1), wallet(2));
            let mut fake = Fake::new(Some(1));
            fake.latest_ranking = vec![row(1, 1, bench)];
            fake.history_ok = false;
            let h = harness(fake, &[idle]).await;
            h.paper_state.set_cursor(&idle, NOW - 300_000).unwrap();
            let (mut evicted, mut marker) = (HashSet::new(), Some(1));

            h.tick(MembershipMode::Knockout, &mut evicted, &mut marker)
                .await;

            assert!(h.controls().is_empty());
            assert!(
                members(&h.live).is_empty(),
                "decided eviction must still apply"
            );
            assert!(evicted.contains(&idle));
            assert_eq!(h.paper_state.cursor(&bench).unwrap(), None);
        }

        #[test]
        fn preparation_set_equals_published_set_for_duplicates_past_the_cap() {
            // `[A, A, B]` at cap 2: the top-`cap` slice holds only A, but `planned_admissions`
            // walks the whole slice and admits B too. One owner computes both sides, so the
            // preparer installs B before the structural apply publishes it.
            let (a, b) = (wallet(1), wallet(2));
            let incoming = vec![entry(a), entry(a), entry(b)];
            let (dropped, admissions) = ranked_membership_change(&[], &incoming, 2);
            assert!(dropped.is_empty());
            assert_eq!(admissions, vec![a, b]);
            let live = live(&[]);
            assert_eq!(live.replace(&HashSet::new(), &incoming, 2), 2);
            assert_eq!(members(&live), set(&[a, b]));
        }

        #[tokio::test]
        async fn capacity_transition_forces_a_resync_to_the_newest_batch() {
            // Capacity may have published rows read from `latest_ranking` before the batch
            // this loop last applied. The changed capacity generation makes the next tick
            // re-apply the newest batch — preparing whatever that adds — then go quiet. The
            // re-sync is not a batch transition: the eviction memory survives it.
            let (a, b, knocked_out) = (wallet(1), wallet(2), wallet(9));
            let mut fake = Fake::new(Some(2));
            fake.ranking_entries = vec![row(2, 1, a), row(2, 2, b)];
            let (activity_hits, position_hits) = (
                Arc::clone(&fake.activity_hits),
                Arc::clone(&fake.position_hits),
            );
            let h = harness(fake, &[a]).await;
            let mut evicted = set(&[knocked_out]);
            let mut sync = BatchSync {
                parking_batch: None,
                reentries_first: true,
                attempted_batch_id: None,
                cooldowns: HashMap::new(),
                started: 0,
                accepted: 0,
                deferred: 0,
                unstarted: 0,
                marker: Some(2),
                capacity_generation: 0,
                knockout_deferred: HashSet::new(),
            };
            h.applied.store(WatchlistCapacityEpoch {
                generation: 7,
                target: CAP,
            });

            h.tick_synced(MembershipMode::FullRerank, &mut evicted, &mut sync)
                .await;
            assert_eq!(h.controls(), vec![(set(&[b]), set(&[a]))]);
            assert_eq!(members(&h.live), set(&[a, b]));
            assert_eq!((sync.marker, sync.capacity_generation), (Some(2), 7));
            assert_eq!(
                evicted,
                set(&[knocked_out]),
                "a re-sync must keep eviction memory"
            );

            let before = (
                activity_hits.load(Ordering::SeqCst),
                position_hits.load(Ordering::SeqCst),
            );
            h.tick_synced(MembershipMode::FullRerank, &mut evicted, &mut sync)
                .await;
            assert_eq!(h.controls().len(), 1, "a synced tick must not re-prepare");
            assert_eq!(
                (
                    activity_hits.load(Ordering::SeqCst),
                    position_hits.load(Ordering::SeqCst)
                ),
                before
            );
        }

        #[tokio::test]
        async fn knockout_batch_transition_clears_eviction_memory_despite_capacity_change() {
            // In knockout mode a capacity change is irrelevant to batch tracking: a genuine new
            // batch observed on the same tick still clears the eviction memory, exactly as
            // before, because the marker was never erased.
            let (live_wallet, knocked_out) = (wallet(1), wallet(9));
            let fake = Fake::new(Some(2));
            let h = harness(fake, &[live_wallet]).await;
            let mut evicted = set(&[knocked_out]);
            let mut sync = BatchSync {
                parking_batch: None,
                reentries_first: true,
                attempted_batch_id: None,
                cooldowns: HashMap::new(),
                started: 0,
                accepted: 0,
                deferred: 0,
                unstarted: 0,
                marker: Some(1),
                capacity_generation: 0,
                knockout_deferred: HashSet::new(),
            };
            h.applied.store(WatchlistCapacityEpoch {
                generation: 7,
                target: CAP,
            });

            h.tick_synced(MembershipMode::Knockout, &mut evicted, &mut sync)
                .await;
            assert!(evicted.is_empty(), "a new batch clears eviction memory");
            assert_eq!((sync.marker, sync.capacity_generation), (Some(2), 7));
            assert!(h.controls().is_empty());
        }

        #[tokio::test]
        async fn eviction_only_tick_issues_no_preparation_requests() {
            let idle = wallet(1);
            let fake = Fake::new(Some(1));
            let (activity_hits, position_hits) = (
                Arc::clone(&fake.activity_hits),
                Arc::clone(&fake.position_hits),
            );
            let h = harness(fake, &[idle]).await;
            h.paper_state.set_cursor(&idle, NOW - 300_000).unwrap();
            let (mut evicted, mut marker) = (HashSet::new(), Some(1));

            h.tick(MembershipMode::Knockout, &mut evicted, &mut marker)
                .await;

            assert!(h.controls().is_empty());
            assert!(members(&h.live).is_empty());
            assert_eq!(activity_hits.load(Ordering::SeqCst), 0);
            assert_eq!(position_hits.load(Ordering::SeqCst), 0);
        }
        fn fence(h: &Harness, wallet: WalletAddress) {
            let conn = rusqlite::Connection::open(h._temp.path().join("paper.db")).unwrap();
            conn.execute("INSERT INTO wallet_fences (wallet_hex, source_trade_id, cause, proof_json, fenced_at_unix) VALUES (?1, 'test', 'invalid_mapping', '{}', 1)", [wallet.to_string()]).unwrap();
        }

        #[tokio::test]
        async fn shared_full_rerank_failure_still_applies_due_demotion() {
            for failure in ["structural"] {
                let (idle, candidate) = (wallet(21), wallet(22));
                let mut fake = Fake::new(Some(2));
                fake.ranking_entries = vec![row(2, 1, candidate)];
                fake.failure = Some(failure);
                let h = harness(fake, &[idle]).await;
                h.paper_state.set_cursor(&idle, NOW - 300_000).unwrap();
                let remembered = wallet(23);
                let (mut evicted, mut marker) = (set(&[remembered]), Some(1));
                h.tick(MembershipMode::FullRerank, &mut evicted, &mut marker)
                    .await;
                assert!(
                    members(&h.live).is_empty(),
                    "{failure}: independently due demotion was skipped"
                );
                assert_eq!(marker, Some(1), "{failure}: unapplied marker advanced");
                assert!(evicted.contains(&idle));
                assert!(
                    evicted.contains(&remembered),
                    "{failure}: eviction memory was cleared"
                );
                assert!(!evicted.contains(&candidate));
            }
        }

        #[tokio::test]
        async fn missing_validation_defers_candidate_and_publishes_ranked_removal() {
            let (old, candidate) = (wallet(24), wallet(25));
            let mut fake = Fake::new(Some(2));
            fake.ranking_entries = vec![row(2, 1, candidate)];
            fake.failure = Some("artifact");
            let h = harness(fake, &[old]).await;
            let (mut evicted, mut marker) = (HashSet::new(), Some(1));

            h.tick(MembershipMode::FullRerank, &mut evicted, &mut marker)
                .await;

            assert!(members(&h.live).is_empty());
            assert_eq!(marker, Some(2));
            assert_eq!(h.membership_publications.load(Ordering::SeqCst), 1);
            let audits = pe_event_log::Reader::replay(h._temp.path().join("source.log"))
                .unwrap()
                .filter_map(Result::ok)
                .filter(|(_, frame)| {
                    frame.source_id.0 == crate::watchlist_admission::MEMBERSHIP_DEFERRAL_SOURCE_ID
                })
                .map(|(_, frame)| {
                    serde_json::from_slice::<serde_json::Value>(&frame.payload).unwrap()
                })
                .collect::<Vec<_>>();
            assert_eq!(audits.len(), 1);
            assert_eq!(audits[0]["deferrals"][0]["wallet"], candidate.to_string());
            assert_eq!(
                audits[0]["deferrals"][0]["kind"],
                "proof.missing_validation"
            );
            assert_eq!(audits[0]["outcome"]["type"], "published");
        }

        #[tokio::test]
        async fn proof_preflight_shared_abort_audits_prior_wallet_without_admission_artifacts() {
            let (old, missing, bad_read) = (wallet(26), wallet(27), wallet(28));
            let mut fake = Fake::new(Some(2));
            fake.ranking_entries = vec![row(2, 1, missing), row(2, 2, bad_read)];
            fake.failure = Some("proof_mixed");
            let h = harness(fake, &[old]).await;
            let (mut evicted, mut marker) = (HashSet::new(), Some(1));

            h.tick(MembershipMode::FullRerank, &mut evicted, &mut marker)
                .await;

            assert_eq!(marker, Some(1));
            assert_eq!(members(&h.live), set(&[old]));
            assert_eq!(h.membership_publications.load(Ordering::SeqCst), 0);
            let frames = pe_event_log::Reader::replay(h._temp.path().join("source.log"))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert!(frames.iter().all(|(_, frame)| {
                frame.source_id.0 != crate::watchlist_admission::MEMBERSHIP_ADMISSION_SOURCE_ID
            }));
            let audits = frames
                .iter()
                .filter(|(_, frame)| {
                    frame.source_id.0 == crate::watchlist_admission::MEMBERSHIP_DEFERRAL_SOURCE_ID
                })
                .map(|(_, frame)| {
                    serde_json::from_slice::<serde_json::Value>(&frame.payload).unwrap()
                })
                .collect::<Vec<_>>();
            assert_eq!(audits.len(), 1);
            assert_eq!(audits[0]["outcome"]["type"], "aborted_shared");
            assert_eq!(audits[0]["deferrals"][0]["wallet"], missing.to_string());
            assert_eq!(
                audits[0]["deferrals"][0]["kind"],
                "proof.missing_validation"
            );
        }

        #[tokio::test]
        async fn mixed_wallet_and_shared_preparation_abort_keeps_marker_and_audits_wallet() {
            let (old, missing, shared) = (wallet(29), wallet(30), wallet(31));
            let mut fake = Fake::new(Some(2));
            fake.ranking_entries = vec![row(2, 1, missing), row(2, 2, shared)];
            fake.history_missing.insert(missing);
            fake.failure = Some("shared_prepare");
            let h = harness(fake, &[old]).await;
            let (mut evicted, mut marker) = (HashSet::new(), Some(1));

            h.tick(MembershipMode::FullRerank, &mut evicted, &mut marker)
                .await;

            assert_eq!(marker, Some(1));
            assert_eq!(members(&h.live), set(&[old]));
            assert_eq!(h.membership_publications.load(Ordering::SeqCst), 0);
            let audits = pe_event_log::Reader::replay(h._temp.path().join("source.log"))
                .unwrap()
                .filter_map(Result::ok)
                .filter(|(_, frame)| {
                    frame.source_id.0 == crate::watchlist_admission::MEMBERSHIP_DEFERRAL_SOURCE_ID
                })
                .map(|(_, frame)| {
                    serde_json::from_slice::<serde_json::Value>(&frame.payload).unwrap()
                })
                .collect::<Vec<_>>();
            assert_eq!(audits.len(), 1);
            assert_eq!(audits[0]["outcome"]["type"], "aborted_shared");
            assert_eq!(audits[0]["deferrals"][0]["wallet"], missing.to_string());
            assert_eq!(audits[0]["deferrals"][0]["kind"], "history.missing");
        }

        #[tokio::test]
        async fn mapping_deferral_and_shared_venue_failure_abort_actual_rerank_tick() {
            let (old, missing, shared) = (wallet(32), wallet(33), wallet(34));
            let mut fake = Fake::new(Some(2));
            fake.ranking_entries = vec![row(2, 1, missing), row(2, 2, shared)];
            let h = harness(fake, &[old]).await;
            let preparer = validator_preparer(&h, Some(missing), set(&[shared]));
            let (mut evicted, mut marker) = (HashSet::new(), Some(1));

            h.tick_with_preparer(
                &preparer,
                MembershipMode::FullRerank,
                &mut evicted,
                &mut marker,
            )
            .await;

            assert_eq!(marker, Some(1));
            assert_eq!(members(&h.live), set(&[old]));
            assert_eq!(h.membership_publications.load(Ordering::SeqCst), 0);
            let frames = pe_event_log::Reader::replay(h._temp.path().join("source.log"))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert!(frames.iter().all(|(_, frame)| {
                frame.source_id.0 != crate::watchlist_admission::MEMBERSHIP_ADMISSION_SOURCE_ID
            }));
            let audits = frames
                .iter()
                .filter(|(_, frame)| {
                    frame.source_id.0 == crate::watchlist_admission::MEMBERSHIP_DEFERRAL_SOURCE_ID
                })
                .map(|(_, frame)| {
                    serde_json::from_slice::<serde_json::Value>(&frame.payload).unwrap()
                })
                .collect::<Vec<_>>();
            assert_eq!(audits.len(), 1);
            assert_eq!(audits[0]["outcome"]["type"], "aborted_shared");
            assert_eq!(audits[0]["deferrals"][0]["wallet"], missing.to_string());
            assert_eq!(
                audits[0]["deferrals"][0]["kind"],
                "positions.missing_activity_mapping"
            );
        }

        #[tokio::test]
        async fn shared_venue_outage_keeps_membership_and_emits_no_deferral_audit() {
            let (old, first, second) = (wallet(35), wallet(36), wallet(37));
            let mut fake = Fake::new(Some(2));
            fake.ranking_entries = vec![row(2, 1, first), row(2, 2, second)];
            let h = harness(fake, &[old]).await;
            let preparer = validator_preparer(&h, None, set(&[first, second]));
            let (mut evicted, mut marker) = (HashSet::new(), Some(1));
            let log_bytes = Arc::new(StdMutex::new(Vec::new()));
            let subscriber = tracing_subscriber::fmt()
                .json()
                .with_max_level(tracing::Level::ERROR)
                .with_writer(CapturedLogs(Arc::clone(&log_bytes)))
                .finish();
            let subscriber_guard = tracing::subscriber::set_default(subscriber);

            h.tick_with_preparer(
                &preparer,
                MembershipMode::FullRerank,
                &mut evicted,
                &mut marker,
            )
            .await;
            drop(subscriber_guard);

            assert_eq!(marker, Some(1));
            assert_eq!(members(&h.live), set(&[old]));
            assert_eq!(h.membership_publications.load(Ordering::SeqCst), 0);
            let captured = String::from_utf8(
                log_bytes
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone(),
            )
            .unwrap();
            let errors = captured
                .lines()
                .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
                .filter(|event| {
                    event["target"] == "pe_service::watchlist_maintenance"
                        && event["fields"]["message"] == "full_rerank: shared admission failure"
                        && event["fields"]["kind"] == "source.transient"
                })
                .collect::<Vec<_>>();
            assert_eq!(errors.len(), 1, "captured logs: {captured}");
            assert_eq!(errors[0]["level"], "ERROR");
            assert_eq!(errors[0]["fields"]["batch_id"], 2);
            assert!(
                errors[0]["fields"]["cause"]
                    .as_str()
                    .is_some_and(|cause| cause.contains("exhausted venue fixture"))
            );
            assert_eq!(
                pe_event_log::Reader::replay(h._temp.path().join("source.log"))
                    .unwrap()
                    .filter_map(Result::ok)
                    .filter(|(_, frame)| {
                        frame.source_id.0
                            == crate::watchlist_admission::MEMBERSHIP_DEFERRAL_SOURCE_ID
                    })
                    .count(),
                0
            );
        }

        #[tokio::test]
        async fn knockout_selection_preserves_inactivity_and_eviction_exclusions() {
            let (idle, live, remembered, fenced, stale, missing, eligible, newer) = (
                wallet(31),
                wallet(32),
                wallet(33),
                wallet(34),
                wallet(35),
                wallet(36),
                wallet(37),
                wallet(38),
            );
            let mut fake = Fake::new(Some(1));
            fake.ranking_entries = vec![
                row(1, 1, idle),
                row(1, 2, live),
                row(1, 3, remembered),
                row(1, 4, fenced),
                row(1, 5, stale),
                row(1, 6, missing),
                row(1, 7, eligible),
                row(2, 1, newer),
            ];
            fake.ranking_entries[4]["last_trade_unix"] =
                serde_json::json!(NOW - supabase_reader::ACTIVE_WINDOW_HOURS * 3600 - 1);
            fake.ranking_entries[5]["last_trade_unix"] = serde_json::Value::Null;
            fake.latest_ranking = vec![row(2, 1, newer)];
            let h = harness(fake, &[idle, live]).await;
            fence(&h, fenced);
            h.paper_state.set_cursor(&idle, NOW - 300_000).unwrap();
            h.paper_state.set_cursor(&live, NOW - 1).unwrap();
            let (mut evicted, mut marker) = (set(&[remembered]), Some(1));
            h.tick(MembershipMode::Knockout, &mut evicted, &mut marker)
                .await;
            assert_eq!(members(&h.live), set(&[live, eligible]));
            assert_eq!(marker, Some(1));
            assert_eq!(evicted, set(&[remembered, idle]));
            assert_eq!(h.paper_state.cursor(&eligible).unwrap(), Some(NOW - 60));
        }
        #[tokio::test]
        async fn empty_selection_preserves_each_consumer_failure_contract() {
            for mode in [MembershipMode::FullRerank, MembershipMode::Knockout] {
                let (idle, fenced) = (wallet(41), wallet(42));
                let mut fake = Fake::new(Some(2));
                fake.ranking_entries = vec![row(2, 1, fenced)];
                let activity_hits = fake.activity_hits.clone();
                let h = harness(fake, &[idle]).await;
                fence(&h, fenced);
                h.paper_state.set_cursor(&idle, NOW - 300_000).unwrap();
                let (mut evicted, mut marker) = (HashSet::new(), Some(1));
                h.tick(mode, &mut evicted, &mut marker).await;
                assert!(members(&h.live).is_empty(), "{mode:?}");
                assert_eq!(marker, Some(2));
                assert!(h.controls().is_empty());
                assert_eq!(activity_hits.load(Ordering::SeqCst), 0);
                assert_eq!(evicted.contains(&idle), mode == MembershipMode::Knockout);
                assert!(h.paper_state.cursor(&fenced).unwrap().is_none());
            }
        }
        fn admission_sync(marker: i64) -> BatchSync {
            BatchSync {
                cooldowns: HashMap::new(),
                parking_batch: Some(marker),
                reentries_first: true,
                attempted_batch_id: None,
                started: 0,
                accepted: 0,
                deferred: 0,
                unstarted: 0,
                marker: Some(marker),
                capacity_generation: 0,
                knockout_deferred: HashSet::new(),
            }
        }

        // Keep Tokio from auto-advancing through local HTTP deadlines. Tests explicitly
        // advance the paused clock at the simulated read/completion boundaries.
        struct PausedIo(tokio::task::JoinHandle<()>);
        impl Drop for PausedIo {
            fn drop(&mut self) {
                self.0.abort();
            }
        }
        fn paused_io() -> PausedIo {
            PausedIo(tokio::spawn(async {
                loop {
                    tokio::task::yield_now().await;
                }
            }))
        }

        struct AdmissionFetcher {
            paper: Arc<PaperStateDb>,
            busy: Arc<StdMutex<HashSet<WalletAddress>>>,
            activity_reads: StdMutex<HashMap<WalletAddress, usize>>,
            spend_budget: bool,
        }
        impl PageFetcher for AdmissionFetcher {
            async fn fetch_page(&self, url: &str) -> Result<Vec<u8>, SourceError> {
                let wallet = url
                    .split("user=")
                    .nth(1)
                    .unwrap()
                    .split('&')
                    .next()
                    .unwrap();
                let wallet = WalletAddress::from_hex(wallet).unwrap();
                if url.contains("/activity?") {
                    let first = {
                        let mut reads = self.activity_reads.lock().unwrap();
                        let count = reads.entry(wallet).or_default();
                        let first = (*count).is_multiple_of(3);
                        *count += 1;
                        first
                    };
                    if first && self.spend_budget {
                        tokio::time::advance(Duration::from_secs(11)).await;
                    }
                } else if url.contains("redeemable=false")
                    && self.busy.lock().unwrap().contains(&wallet)
                {
                    // Simulate ordinary reconciliation changing the delivery cursor between
                    // captures. The real validator classifies this as intervening activity.
                    let at = self.paper.cursor(&wallet).unwrap().unwrap_or(NOW - 60);
                    self.paper.set_cursor(&wallet, at + 1).unwrap();
                }
                Ok(b"[]".to_vec())
            }
        }

        fn admission_preparer(
            h: &Harness,
            busy: Arc<StdMutex<HashSet<WalletAddress>>>,
            spend_budget: bool,
        ) -> AdmissionPreparer {
            let fetcher: Arc<dyn ReconciliationFetcher> = Arc::new(AdmissionFetcher {
                paper: h.paper_state.clone(),
                busy,
                activity_reads: StdMutex::new(HashMap::new()),
                spend_budget,
            });
            let identity = Arc::new(AssetIdentityResolver::new(
                fetcher.clone(),
                "https://fixture.invalid".to_owned(),
                GAMMA_BATCH_SIZE,
                Arc::new(tokio::sync::Mutex::new(
                    SourceEventSink::open(h._temp.path().join("admission-identity.log")).unwrap(),
                )),
            ));
            AdmissionPreparer::with_validator(
                h.control_tx.clone(),
                h.paper_state.clone(),
                CausalPositionValidator::new(
                    fetcher,
                    "https://fixture.invalid",
                    "admission-test",
                    identity,
                )
                .with_clock(Arc::new(|| NOW)),
            )
            .with_source_log(h.source_handle.clone())
        }

        async fn admission_tick(h: &Harness, preparer: &AdmissionPreparer, sync: &mut BatchSync) {
            admission_tick_with_interval(h, preparer, sync, 10).await;
        }

        async fn admission_tick_with_interval(
            h: &Harness,
            preparer: &AdmissionPreparer,
            sync: &mut BatchSync,
            interval_secs: u64,
        ) {
            admission_tick_in_mode(h, preparer, sync, MembershipMode::FullRerank, interval_secs)
                .await;
        }

        async fn admission_tick_in_mode(
            h: &Harness,
            preparer: &AdmissionPreparer,
            sync: &mut BatchSync,
            membership_mode: MembershipMode,
            interval_secs: u64,
        ) {
            let cfg = MaintenanceConfig {
                interval_secs,
                membership_mode,
                ..cfg()
            };
            maintenance_tick(
                &h.live,
                &h.paper_state,
                &h.client,
                &h.base_url,
                "anon",
                "",
                &h.writer_lock,
                &h.applied,
                preparer,
                &cfg,
                h.applied.load(),
                &mut HashSet::new(),
                sync,
                NOW,
            )
            .await;
        }

        fn log_fields(bytes: &Arc<StdMutex<Vec<u8>>>, message: &str) -> Vec<serde_json::Value> {
            String::from_utf8(bytes.lock().unwrap().clone())
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
                .filter(|line| line["fields"]["message"] == message)
                .map(|line| line["fields"].clone())
                .collect()
        }
        fn launched(log: &serde_json::Value) -> Vec<(String, u64)> {
            serde_json::from_str(log["started_previous_keys"].as_str().unwrap()).unwrap()
        }
        fn eligible(log: &serde_json::Value) -> Vec<String> {
            serde_json::from_str(log["eligible"].as_str().unwrap()).unwrap()
        }

        #[tokio::test(start_paused = true)]
        async fn admission_retry_eligibility_boundary() {
            let dir = tempfile::tempdir().unwrap();
            let paper = PaperStateDb::open(&dir.path().join("paper.db")).unwrap();
            let wallet = wallet(1);
            let mut sync = admission_sync(1);
            let completion = tokio::time::Instant::now();
            tokio::time::advance(Duration::from_secs(30)).await;
            sync.completed(
                &paper,
                &[wallet],
                &[],
                &[crate::watchlist_admission::Deferral {
                    completed_at: Some(completion),
                    wallet,
                    stage: "validation",
                    class: crate::position_seeder::FailureClass::WalletTransient,
                    kind: "validation.intervening_activity",
                    message: "busy".to_owned(),
                }],
                &[],
            );
            assert_eq!(
                sync.cooldowns[&wallet],
                completion + Duration::from_secs(300)
            );
            tokio::time::advance(Duration::from_secs(269)).await;
            assert!(sync.cooling(&wallet));
            tokio::time::advance(Duration::from_secs(1)).await;
            assert!(!sync.cooling(&wallet));
        }

        #[tokio::test(start_paused = true)]
        async fn maintenance_first_tick_at_startup() {
            let _io = paused_io();
            let wallet = wallet(1);
            let mut fake = Fake::new(Some(1));
            fake.ranking_entries = vec![row(1, 1, wallet)];
            // The production loop samples UTC; only its interval clock is paused.
            fake.ranking_entries[0]["last_trade_unix"] =
                serde_json::json!(OffsetDateTime::now_utc().unix_timestamp() - 60);
            let mut h = harness(fake, &[wallet]).await;
            h.live.remove_fenced(&set(&[wallet]));
            h.projection_rx.borrow_and_update();
            let start = tokio::time::Instant::now();
            let task = tokio::spawn(run_maintenance_loop(
                h.live.clone(),
                h.paper_state.clone(),
                h.client.clone(),
                h.base_url.clone(),
                "anon".to_owned(),
                "".to_owned(),
                Arc::new(Mutex::new(())),
                cfg(),
                h.applied.clone(),
                h.preparer.clone(),
                Some(1),
                HashSet::new(),
                HashMap::new(),
            ));
            while !members(&h.live).contains(&wallet) {
                h.projection_rx.changed().await.unwrap();
            }
            assert_eq!(members(&h.live), set(&[wallet]));
            assert_eq!(h.controls().len(), 1);
            assert_eq!(tokio::time::Instant::now(), start);
            task.abort();
            let _ = task.await;
        }

        #[tokio::test(start_paused = true)]
        async fn admission_queue_keys_bound_failing_wallets() {
            let _io = paused_io();
            let wallets = (1..=5).map(wallet).collect::<Vec<_>>();
            let mut fake = Fake::new(Some(1));
            for batch in 1..=5 {
                let shift = usize::try_from(batch).unwrap() % wallets.len();
                for (rank, wallet) in wallets.iter().cycle().skip(shift).take(5).enumerate() {
                    fake.ranking_entries.push(row(
                        batch,
                        i64::try_from(rank + 1).unwrap(),
                        *wallet,
                    ));
                }
            }
            let latest = fake.latest_batch.clone();
            let mut h = harness(fake, &wallets).await;
            h.applied = AppliedWatchlistCapacity::new(5);
            h.live.remove_fenced(&set(&wallets));
            let busy = Arc::new(StdMutex::new(set(&wallets)));
            let preparer = admission_preparer(&h, busy.clone(), true);
            let initial = preparer.prepare(&wallets).await.unwrap();
            assert_eq!(initial.started, wallets);
            assert_eq!(initial.deferred.len(), 5);
            assert!(
                initial
                    .deferred
                    .iter()
                    .all(|d| d.kind == "validation.intervening_activity")
            );
            busy.lock().unwrap().remove(&wallets[4]);
            let bytes = Arc::new(StdMutex::new(Vec::new()));
            let _logs = tracing::subscriber::set_default(
                tracing_subscriber::fmt()
                    .json()
                    .with_writer(CapturedLogs(bytes.clone()))
                    .finish(),
            );
            let mut sync = admission_sync(1);
            let mut first_turns = 0;
            for batch in 2..=5 {
                latest.store(batch, Ordering::SeqCst);
                // Apply the reordered batch with no launch budget before each measured pass.
                // These zero-start calls must retain all existing queue keys.
                admission_tick_with_interval(&h, &preparer, &mut sync, 0).await;
                assert_eq!(sync.marker, Some(batch));
                assert_eq!(sync.started, 0);
                tokio::time::advance(Duration::from_secs(300)).await;
                admission_tick(&h, &preparer, &mut sync).await;
                assert!(sync.reentries_first);
                if sync.started > 0 {
                    first_turns += 1;
                }
                if members(&h.live).contains(&wallets[4]) {
                    break;
                }
            }
            assert!(members(&h.live).contains(&wallets[4]));
            assert_eq!(first_turns, 2, "ceil(5/4) first-claim ticks");
            let logs = log_fields(&bytes, "admission launch order");
            assert!(logs.iter().any(|l| {
                launched(l)
                    .iter()
                    .any(|(w, _)| *w == wallets[4].to_string())
            }));
            assert!(
                logs.iter()
                    .filter(|l| l["path"] == "reentry" && l["started"] != 0)
                    .all(|l| l["started"].as_u64().unwrap() <= 4)
            );
        }

        #[tokio::test(start_paused = true)]
        async fn admission_newcomers_queue_behind_waiting_wallet() {
            let _io = paused_io();
            let old = wallet(1);
            let newcomers = [wallet(2), wallet(3), wallet(4)];
            let mut fake = Fake::new(Some(1));
            fake.ranking_entries = std::iter::once(old)
                .chain(newcomers)
                .enumerate()
                .map(|(i, w)| row(1, i64::try_from(i + 1).unwrap(), w))
                .collect();
            let h = harness(fake, &[]).await;
            let preparer = admission_preparer(&h, Arc::new(StdMutex::new(set(&[old]))), true);
            let seeds = std::iter::once(old)
                .chain(newcomers)
                .map(|w| (w, NOW - 60))
                .collect();
            let offered = preparer
                .prepare_ranked_until(
                    &[old],
                    &seeds,
                    Some(tokio::time::Instant::now()),
                    crate::watchlist_admission::AdmissionContext::Addition { first: true },
                )
                .await
                .unwrap();
            assert_eq!(offered.unstarted, vec![old]);
            for new in newcomers {
                let outcome = preparer
                    .prepare_ranked_until(
                        &[new, old],
                        &seeds,
                        Some(tokio::time::Instant::now() + Duration::from_secs(10)),
                        crate::watchlist_admission::AdmissionContext::Addition { first: true },
                    )
                    .await
                    .unwrap();
                assert_eq!(outcome.started.first(), Some(&old));
                assert_eq!(outcome.deferred[0].wallet, old);
                assert_eq!(outcome.deferred[0].kind, "validation.intervening_activity");
            }
        }

        #[tokio::test(start_paused = true)]
        async fn admission_paths_alternate_first_claim() {
            let _io = paused_io();
            for mode in [MembershipMode::FullRerank, MembershipMode::Knockout] {
                let retained = wallet(1);
                let additions = (2..=9).map(wallet).collect::<Vec<_>>();
                let mut fake = Fake::new(Some(2));
                fake.ranking_entries.push(row(1, 1, retained));
                for batch in 2..=3 {
                    fake.ranking_entries.push(row(batch, 1, retained));
                    let start = usize::try_from(batch - 2).unwrap() * 4;
                    for (i, w) in additions[start..start + 4].iter().enumerate() {
                        fake.ranking_entries
                            .push(row(batch, i64::try_from(i + 2).unwrap(), *w));
                    }
                }
                let latest = fake.latest_batch.clone();
                let mut h = harness(fake, &[retained]).await;
                h.applied = AppliedWatchlistCapacity::new(5);
                h.live.remove_fenced(&set(&[retained]));
                let preparer =
                    admission_preparer(&h, Arc::new(StdMutex::new(set(&additions))), true);
                let mut sync = admission_sync(1);
                admission_tick_in_mode(&h, &preparer, &mut sync, mode, 10).await;
                assert!(!sync.reentries_first);
                assert!(!members(&h.live).contains(&retained));
                assert_eq!(sync.started, 4);
                latest.store(3, Ordering::SeqCst);
                admission_tick_in_mode(&h, &preparer, &mut sync, mode, 10).await;
                assert!(sync.reentries_first);
                assert!(
                    members(&h.live).contains(&retained),
                    "retained re-entry claims the second tick's budget"
                );
                assert_eq!(sync.started, 1);
            }
        }

        #[tokio::test(start_paused = true)]
        async fn knockout_backfill_alternates_first_claim() {
            let _io = paused_io();
            let (retained, bench) = (wallet(1), wallet(2));
            let mut fake = Fake::new(Some(1));
            fake.latest_ranking = vec![row(1, 1, retained), row(1, 2, bench)];
            let mut h = harness(fake, &[retained]).await;
            h.applied = AppliedWatchlistCapacity::new(2);
            h.live.remove_fenced(&set(&[retained]));
            let preparer = admission_preparer(&h, Arc::new(StdMutex::new(set(&[retained]))), true);
            let bytes = Arc::new(StdMutex::new(Vec::new()));
            let _logs = tracing::subscriber::set_default(
                tracing_subscriber::fmt()
                    .json()
                    .with_writer(CapturedLogs(bytes.clone()))
                    .finish(),
            );
            let mut sync = admission_sync(1);
            sync.reentries_first = false;
            let mut evicted = HashSet::new();
            let config = MaintenanceConfig {
                interval_secs: 10,
                membership_mode: MembershipMode::Knockout,
                ..cfg()
            };
            for tick in 0..2 {
                maintenance_tick(
                    &h.live,
                    &h.paper_state,
                    &h.client,
                    &h.base_url,
                    "anon",
                    "",
                    &h.writer_lock,
                    &h.applied,
                    &preparer,
                    &config,
                    h.applied.load(),
                    &mut evicted,
                    &mut sync,
                    NOW,
                )
                .await;
                assert_eq!(sync.marker, Some(1), "unchanged batch");
                if tick == 0 {
                    assert!(!members(&h.live).contains(&bench));
                    assert_eq!(sync.started, 1);
                    tokio::time::advance(Duration::from_secs(300)).await;
                }
            }
            assert!(
                members(&h.live).contains(&bench),
                "backfill claims its second tick"
            );
            assert!(!members(&h.live).contains(&retained));
            let logs = log_fields(&bytes, "admission launch order");
            assert_eq!(
                logs.iter()
                    .map(|l| (l["path"].as_str().unwrap(), l["first"].as_bool().unwrap()))
                    .collect::<Vec<_>>(),
                vec![
                    ("reentry", true),
                    ("addition", false),
                    ("addition", false),
                    ("addition", true),
                    ("reentry", false)
                ]
            );
            let started = logs
                .iter()
                .filter(|l| l["started"] != 0)
                .collect::<Vec<_>>();
            assert_eq!(launched(started[0])[0].0, retained.to_string());
            assert_eq!(launched(started[1])[0].0, bench.to_string());
        }

        #[tokio::test(start_paused = true)]
        async fn admission_batch_ids_attempted_and_applied() {
            let _io = paused_io();
            let mut fake = Fake::new(Some(2));
            fake.ranking_entries = vec![row(2, 1, wallet(2))];
            fake.failure = Some("structural");
            let latest = fake.latest_batch.clone();
            let h = harness(fake, &[wallet(1)]).await;
            let bytes = Arc::new(StdMutex::new(Vec::new()));
            let _logs = tracing::subscriber::set_default(
                tracing_subscriber::fmt()
                    .json()
                    .with_writer(CapturedLogs(bytes.clone()))
                    .finish(),
            );
            let mut sync = admission_sync(1);
            admission_tick(&h, &h.preparer, &mut sync).await;
            assert_eq!(sync.attempted_batch_id, Some(2));
            assert_eq!(sync.marker, Some(1));
            latest.store(1, Ordering::SeqCst);
            admission_tick(&h, &h.preparer, &mut sync).await;
            assert!(sync.attempted_batch_id.is_none());
            let logs = log_fields(&bytes, "maintenance admission budget completed");
            assert_eq!(logs.len(), 2);
            assert_eq!(logs[0]["attempted_batch_id"], 2);
            assert_eq!(logs[0]["applied_batch_id"], 1);
            assert_eq!(logs[0]["capacity_generation"], 0);
            assert!(logs[1].get("attempted_batch_id").is_none());
        }

        #[tokio::test(start_paused = true)]
        async fn admission_queue_keys_survive_resync() {
            let _io = paused_io();
            let (waiting, first, second, third) = (wallet(1), wallet(2), wallet(3), wallet(4));
            let mut fake = Fake::new(Some(2));
            fake.failure = Some("structural_after_first");
            fake.ranking_entries = vec![
                row(1, 1, waiting),
                row(2, 1, waiting),
                row(2, 2, first),
                row(3, 1, waiting),
                row(3, 2, second),
                row(4, 1, waiting),
                row(4, 2, third),
            ];
            let latest = fake.latest_batch.clone();
            let h = harness(fake, &[waiting]).await;
            h.live.remove_fenced(&set(&[waiting]));
            // A zero-start offer gives this wallet a key; synchronization must never discard it.
            let seeds = HashMap::from([(waiting, NOW - 60)]);
            h.preparer
                .prepare_ranked_until(
                    &[waiting],
                    &seeds,
                    Some(tokio::time::Instant::now()),
                    crate::watchlist_admission::AdmissionContext::Other,
                )
                .await
                .unwrap();
            let bytes = Arc::new(StdMutex::new(Vec::new()));
            let _logs = tracing::subscriber::set_default(
                tracing_subscriber::fmt()
                    .json()
                    .with_writer(CapturedLogs(bytes.clone()))
                    .finish(),
            );
            let mut sync = admission_sync(1);
            admission_tick(&h, &h.preparer, &mut sync).await;
            assert_eq!(sync.marker, Some(2));
            latest.store(3, Ordering::SeqCst);
            admission_tick(&h, &h.preparer, &mut sync).await;
            assert_eq!(
                sync.marker,
                Some(2),
                "failed publication keeps applied batch"
            );
            latest.store(2, Ordering::SeqCst);
            h.applied.store(WatchlistCapacityEpoch {
                generation: 1,
                target: CAP,
            });
            admission_tick(&h, &h.preparer, &mut sync).await;
            assert_eq!(sync.capacity_generation, 1);
            let outcome = h
                .preparer
                .prepare(&[third, second, waiting, first])
                .await
                .unwrap();
            assert_eq!(outcome.started, vec![first, waiting, second, third]);
            let logs = log_fields(&bytes, "admission launch order");
            let waiting_keys = logs
                .iter()
                .flat_map(launched)
                .filter(|(w, _)| *w == waiting.to_string())
                .map(|(_, key)| key)
                .collect::<Vec<_>>();
            assert_eq!(waiting_keys.len(), 2);
            assert_eq!(
                waiting_keys[0], 0,
                "first-offer key retained through successful apply"
            );
            assert!(
                waiting_keys[1] > waiting_keys[0],
                "started key survives failed apply and capacity resync"
            );
            for (wallet, expected) in [(first, vec![1, 2]), (second, vec![4, 5])] {
                let keys = logs
                    .iter()
                    .flat_map(launched)
                    .filter(|(w, _)| *w == wallet.to_string())
                    .map(|(_, key)| key)
                    .collect::<Vec<_>>();
                assert_eq!(
                    keys, expected,
                    "successful and failed applications retain started wallets' keys"
                );
            }
        }

        #[tokio::test(start_paused = true)]
        async fn admission_launch_order_both_paths() {
            let _io = paused_io();
            for mode in [MembershipMode::FullRerank, MembershipMode::Knockout] {
                let (reentry, addition, newer) = (wallet(1), wallet(2), wallet(3));
                let mut fake = Fake::new(Some(2));
                fake.ranking_entries = vec![
                    row(1, 1, reentry),
                    row(2, 1, reentry),
                    row(2, 2, addition),
                    row(3, 1, reentry),
                    row(3, 2, addition),
                    row(3, 3, newer),
                ];
                let latest = fake.latest_batch.clone();
                let h = harness(fake, &[reentry]).await;
                h.live.remove_fenced(&set(&[reentry]));
                let bytes = Arc::new(StdMutex::new(Vec::new()));
                let _logs = tracing::subscriber::set_default(
                    tracing_subscriber::fmt()
                        .json()
                        .with_writer(CapturedLogs(bytes.clone()))
                        .finish(),
                );
                let mut sync = admission_sync(1);
                admission_tick_in_mode(&h, &h.preparer, &mut sync, mode, 10).await;
                h.live.remove_fenced(&set(&[reentry]));
                latest.store(3, Ordering::SeqCst);
                admission_tick_in_mode(&h, &h.preparer, &mut sync, mode, 10).await;
                let logs = log_fields(&bytes, "admission launch order");
                let started = logs
                    .iter()
                    .filter(|l| l["started"] != 0)
                    .collect::<Vec<_>>();
                assert_eq!(
                    started
                        .iter()
                        .map(|l| (l["path"].as_str().unwrap(), l["first"].as_bool().unwrap()))
                        .collect::<Vec<_>>(),
                    vec![
                        ("addition", true),
                        ("reentry", false),
                        ("reentry", true),
                        ("addition", false)
                    ]
                );
                for (log, wallet) in started.iter().zip([addition, reentry, reentry, newer]) {
                    assert_eq!(eligible(log), vec![wallet.to_string()]);
                    assert_eq!(launched(log)[0].0, wallet.to_string());
                    assert_eq!(log["started"], 1);
                }
                assert_eq!(
                    h.controls()
                        .iter()
                        .map(|(wallets, _)| *wallets.iter().next().unwrap())
                        .collect::<Vec<_>>(),
                    vec![addition, reentry, reentry, newer]
                );
                // Capacity and direct callers use the same queue, with no tick-first field.
                h.preparer
                    .prepare_ranked_until(
                        &[newer],
                        &HashMap::new(),
                        None,
                        crate::watchlist_admission::AdmissionContext::Capacity,
                    )
                    .await
                    .unwrap();
                h.preparer.prepare(&[addition]).await.unwrap();
                let logs = log_fields(&bytes, "admission launch order");
                assert_eq!(logs[logs.len() - 2]["path"], "capacity");
                assert_eq!(logs[logs.len() - 1]["path"], "other");
                assert!(logs[logs.len() - 2].get("first").is_none());
                assert!(logs[logs.len() - 1].get("first").is_none());
            }
        }

        #[tokio::test(start_paused = true)]
        async fn admission_timeout_and_zero_start_calls() {
            let _io = paused_io();
            let (holder, a, b) = (wallet(1), wallet(2), wallet(3));
            let mut fake = Fake::new(Some(1));
            fake.ranking_entries = vec![row(1, 1, holder), row(1, 2, a), row(1, 3, b)];
            let h = harness(fake, &[]).await;
            let (tx, mut rx) = mpsc::channel(1);
            let preparer = AdmissionPreparer::new(tx, h.paper_state.clone());
            let held_preparer = preparer.clone();
            let holder_task =
                tokio::spawn(async move { held_preparer.prepare(&[holder]).await.unwrap() });
            let OrchestratorControl::PrepareAdmissions { acknowledged, .. } =
                rx.recv().await.unwrap()
            else {
                panic!("expected holder preparation")
            };
            let seeds = HashMap::from([(a, NOW - 60), (b, NOW - 60)]);
            let timed = preparer
                .prepare_ranked_until(
                    &[a, b],
                    &seeds,
                    Some(tokio::time::Instant::now()),
                    crate::watchlist_admission::AdmissionContext::Other,
                )
                .await
                .unwrap();
            assert_eq!(timed.unstarted, vec![a, b]);
            assert!(timed.started.is_empty() && timed.deferred.is_empty());
            assert!(h.paper_state.cursor(&a).unwrap().is_none());
            assert!(h.paper_state.cursor(&b).unwrap().is_none());
            acknowledged.send(()).unwrap();
            holder_task.await.unwrap();
            let bytes = Arc::new(StdMutex::new(Vec::new()));
            let _logs = tracing::subscriber::set_default(
                tracing_subscriber::fmt()
                    .json()
                    .with_writer(CapturedLogs(bytes.clone()))
                    .finish(),
            );
            let zero = preparer
                .prepare_ranked_until(
                    &[b, a],
                    &seeds,
                    Some(tokio::time::Instant::now()),
                    crate::watchlist_admission::AdmissionContext::Other,
                )
                .await
                .unwrap();
            assert_eq!(zero.unstarted, vec![b, a]);
            assert!(zero.started.is_empty() && zero.deferred.is_empty());
            assert_eq!(h.paper_state.cursor(&a).unwrap(), Some(NOW - 60));
            assert_eq!(h.paper_state.cursor(&b).unwrap(), Some(NOW - 60));
            let reseed = HashMap::from([(a, NOW), (b, NOW)]);
            let again = preparer
                .prepare_ranked_until(
                    &[a, b],
                    &reseed,
                    Some(tokio::time::Instant::now()),
                    crate::watchlist_admission::AdmissionContext::Other,
                )
                .await
                .unwrap();
            assert_eq!(again.unstarted, vec![b, a]);
            assert!(again.started.is_empty() && again.deferred.is_empty());
            assert_eq!(h.paper_state.cursor(&a).unwrap(), Some(NOW - 60));
            assert_eq!(h.paper_state.cursor(&b).unwrap(), Some(NOW - 60));
            let zero_logs = log_fields(&bytes, "admission launch order");
            assert_eq!(zero_logs.len(), 2);
            assert!(zero_logs.iter().all(|l| l["started"] == 0
                && launched(l).is_empty()
                && eligible(l) == vec![b.to_string(), a.to_string()]));
            let mut sync = admission_sync(1);
            sync.completed(
                &h.paper_state,
                &zero.started,
                &zero.admitted,
                &zero.deferred,
                &zero.unstarted,
            );
            assert_eq!(sync.started, 0);
            assert_eq!(sync.deferred, 0);
            assert!(sync.cooldowns.is_empty());
            let actor = tokio::spawn(async move {
                while let Some(OrchestratorControl::PrepareAdmissions { acknowledged, .. }) =
                    rx.recv().await
                {
                    acknowledged.send(()).unwrap();
                }
            });
            let outcome = preparer.prepare(&[a, b]).await.unwrap();
            assert_eq!(
                outcome.started,
                vec![b, a],
                "zero-start offers register rank-ordered keys; timeout registered none"
            );
            drop(preparer);
            actor.await.unwrap();
        }

        #[tokio::test(start_paused = true)]
        async fn busy_then_quiet_wallet_admitted() {
            let _io = paused_io();
            let wallet = wallet(1);
            let mut fake = Fake::new(Some(1));
            fake.ranking_entries = vec![row(1, 1, wallet)];
            let h = harness(fake, &[wallet]).await;
            h.live.remove_fenced(&set(&[wallet]));
            let busy = Arc::new(StdMutex::new(set(&[wallet])));
            let preparer = admission_preparer(&h, busy.clone(), true);
            let mut sync = admission_sync(1);
            admission_tick(&h, &preparer, &mut sync).await;
            assert_eq!(sync.started, 1);
            assert_eq!(sync.deferred, 1);
            assert!(members(&h.live).is_empty());
            busy.lock().unwrap().clear();
            tokio::time::advance(Duration::from_secs(300)).await;
            admission_tick(&h, &preparer, &mut sync).await;
            assert_eq!(sync.started, 1);
            assert_eq!(sync.accepted, 1);
            assert_eq!(members(&h.live), set(&[wallet]));
            assert!(!sync.cooldowns.contains_key(&wallet));
        }
    }
    #[tokio::test(start_paused = true)]
    async fn paper_service_rollout_cooldown_uses_terminal_time_and_survives_batch_changes() {
        let dir = tempfile::tempdir().unwrap();
        let paper = PaperStateDb::open(&dir.path().join("paper.db")).unwrap();
        let wallet = WalletAddress([0xab; 20]);
        let mut sync = BatchSync {
            marker: Some(1),
            parking_batch: Some(1),
            reentries_first: true,
            attempted_batch_id: None,
            capacity_generation: 0,
            knockout_deferred: HashSet::new(),
            cooldowns: HashMap::new(),
            started: 0,
            accepted: 0,
            deferred: 0,
            unstarted: 0,
        };
        let terminal = tokio::time::Instant::now();
        tokio::time::advance(Duration::from_secs(30)).await;
        let failure = crate::watchlist_admission::Deferral {
            wallet,
            stage: "validation",
            class: crate::position_seeder::FailureClass::WalletTransient,
            kind: "identity.transient",
            message: "Gamma failed".to_owned(),
            completed_at: Some(terminal),
        };
        sync.completed(&paper, &[wallet], &[], &[failure], &[]);
        assert_eq!(
            sync.cooldowns[&wallet],
            terminal + Duration::from_secs(crate::watchlist_admission::ADMISSION_RETRY_SECS)
        );
        sync.marker = Some(2);
        sync.knockout_deferred.clear();
        assert!(sync.cooling(&wallet));
        tokio::time::advance(Duration::from_secs(
            crate::watchlist_admission::ADMISSION_RETRY_SECS - 30,
        ))
        .await;
        assert!(!sync.cooling(&wallet));
        sync.completed(&paper, &[wallet], &[wallet], &[], &[]);
        assert!(!sync.cooldowns.contains_key(&wallet));
    }
}
