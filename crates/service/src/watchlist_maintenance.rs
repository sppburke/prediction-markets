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
//! The idle clock is the wallet's *real last-trade time* (#357): the poll cursor is seeded from
//! `ranking_entries.last_trade_unix` at admission — here, for each backfilled wallet, inside the
//! writer-locked critical section — and advanced forward by the [`crate::trade_poller`], so
//! `idle = now − cursor = now − real_last_trade`. There is no admission grace: a wallet whose real
//! last trade is already > `inactivity_threshold_secs` ago is eviction-eligible on the next tick
//! (candidates are pre-filtered to < 72h at fetch, so this bites only stale bootstrap admissions).
//! A `cursor == None` wallet (not yet polled and no seed value) is treated as idle 0: it self-heals
//! to "now" rather than being read as inactive-forever, until the poller writes its real last trade.
//!
//! Freed slots are atomically backfilled via [`LiveWatchlist::replace`] from the top of
//! `latest_ranking`, excluding the live ∪ evicted sets. The refresh loop and this tick are
//! serialized by a shared [`tokio::sync::Mutex`] writer lock; readers stay lock-free. The
//! realized-edge series both triggers consume comes from the authoritative local `paper_state.db`
//! (`list_fills` + in-process [`ResolutionStore`]), never the best-effort Supabase mirror.
//!
//! ## Membership modes (2026-07-03 run28 cutover)
//!
//! [`MembershipMode`] selects who owns MEMBERSHIP between ranking batches:
//!
//! * [`MembershipMode::Knockout`] (legacy default) — hold-until-knockout: the ranking push
//!   never changes membership; only the knockout+backfill above does.
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
//! The full-rerank read is pinned to the batch identifier that triggered the transition, so the
//! rows applied and the marker committed always name one batch.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use pe_core_types::WalletAddress;
use pe_paper_pnl::ResolutionStore;
use pe_paper_state::{PaperStateDb, PaperStateError};
use pe_trader_index::{Watchlist, WatchlistEntry};
use rust_decimal::Decimal;
use time::OffsetDateTime;
use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::demotion_stat::{WalletEdgeStats, wallet_edge_stats};
use crate::live_watchlist::LiveWatchlist;
use crate::runtime_config::{AppliedWatchlistCapacity, WatchlistCapacityEpoch};
use crate::supabase_reader;
use crate::watchlist_admission::AdmissionPreparer;

/// Who owns watchlist MEMBERSHIP between ranking batches. See the module docs; canonical
/// default in `docs/_GLOSSARY.md` (`watchlist_membership_mode`). Boot-frozen (env/TOML) —
/// the maintenance loop is built once at startup, so changing the mode needs a restart.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MembershipMode {
    /// Hold-until-knockout (legacy): membership changes only via knockout + bench backfill.
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
    /// Extra bench candidates fetched beyond the freed-slot count.
    pub bench_overfetch: usize,
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
    /// The wallet's real last-trade time (its poll cursor = the inactivity clock) at eviction
    /// (#357); `None` for a never-polled wallet evicted on a non-inactivity trigger.
    pub last_trade_unix: Option<i64>,
}

/// Decide whether a single live wallet is knocked out this tick. Pure.
///
/// Underperformance takes reason-precedence over inactivity: it is the more specific, actionable
/// signal and its trailing-window realized-P&L AND-gate is the safety net for the CB constants.
///
/// # Precondition
/// `last_ts` is the wallet's poll cursor (its real last-trade time, #357). `None` means the wallet
/// has not yet been polled and is treated as just-admitted (idle 0) — never inactive-evicted this
/// tick.
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
/// `cursors` maps each live wallet to its poll cursor (`None` = not yet polled). `stats` is keyed
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
                // The poll cursor IS the real last-trade time (#357) — record it for the audit.
                last_trade_unix: last_ts,
            })
        })
        .collect()
}

fn planned_admissions(
    current: &[WatchlistEntry],
    removed: &HashSet<WalletAddress>,
    candidates: &[WatchlistEntry],
    cap: usize,
) -> Vec<WalletAddress> {
    let mut present: HashSet<WalletAddress> = current
        .iter()
        .filter(|entry| !removed.contains(&entry.wallet))
        .map(|entry| entry.wallet)
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

/// The exact membership change an incoming ranked set produces against `current`: the wallets
/// dropped because they fall outside the incoming top-`cap`, and the wallets newly admitted.
///
/// This is the single owner of that computation (#542): the structural apply publishes exactly
/// this admission set under the writer lock, and the preparer installs exactly this set before
/// the lock is taken, so the two can never disagree. Duplicate wallets and an incoming slice
/// longer than `cap` (neither is produced by the `limit`-bounded ranking reads) resolve the same
/// way on both sides because [`planned_admissions`] and [`LiveWatchlist::replace`] share the
/// same walk.
pub(crate) fn ranked_membership_change(
    current: &[WatchlistEntry],
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
        .map(|entry| entry.wallet)
        .filter(|wallet| !incoming_set.contains(wallet))
        .collect();
    let removed: HashSet<WalletAddress> = dropped.iter().copied().collect();
    let admissions = planned_admissions(current, &removed, incoming, cap);
    (dropped, admissions)
}

fn admission_seeds(
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
    applied_capacity: &AppliedWatchlistCapacity,
    expected_capacity: WatchlistCapacityEpoch,
    removed: &HashSet<WalletAddress>,
    candidates: &[WatchlistEntry],
    candidate_last_trade: &HashMap<WalletAddress, i64>,
) -> Result<usize, MembershipApplyError> {
    let _guard = writer_lock.lock().await;
    let applied = applied_capacity.load();
    if applied != expected_capacity {
        return Err(stale_capacity_error(expected_capacity, applied));
    }
    remove_loaded_fences(live, paper_state)?;
    let current = live.snapshot();
    let admissions = planned_admissions(
        &current.entries,
        removed,
        candidates,
        expected_capacity.target,
    );
    recheck_admissions(paper_state, &admissions)?;
    let seeds = admission_seeds(&admissions, candidate_last_trade)?;
    // #511: insert-only — an existing (possibly HELD) delivery cursor is already a valid
    // lower bound and must never be jumped by a re-admission seed; activity MAX-seeds.
    paper_state.seed_cursors_if_absent(&seeds)?;
    Ok(live.replace(removed, candidates, expected_capacity.target))
}

/// Apply an exact ranked membership while the caller holds the structural-writer mutex.
///
/// Used by both same-cap full reranks and runtime capacity transitions. Cursor persistence is a
/// fail-closed prerequisite to ArcSwap publication.
pub(crate) fn apply_ranked_membership_locked(
    live: &LiveWatchlist,
    paper_state: &PaperStateDb,
    incoming: &[WatchlistEntry],
    incoming_last_trade: &HashMap<WalletAddress, i64>,
    cap: usize,
) -> Result<(usize, Vec<WalletAddress>), MembershipApplyError> {
    remove_loaded_fences(live, paper_state)?;
    let current = live.snapshot();
    let (dropped, admissions) = ranked_membership_change(&current.entries, incoming, cap);
    let removed: HashSet<WalletAddress> = dropped.iter().copied().collect();
    recheck_admissions(paper_state, &admissions)?;
    let seeds = admission_seeds(&admissions, incoming_last_trade)?;
    // #511: insert-only (see membership admission above).
    paper_state.seed_cursors_if_absent(&seeds)?;
    let total = live.replace(&removed, incoming, cap);
    Ok((total, dropped))
}

/// Wholesale membership rotation for [`MembershipMode::FullRerank`]. The operation is rejected
/// if network work was planned against an applied capacity epoch that is no longer current.
pub async fn apply_full_rerank_swap(
    live: &LiveWatchlist,
    paper_state: &PaperStateDb,
    writer_lock: &Mutex<()>,
    applied_capacity: &AppliedWatchlistCapacity,
    expected_capacity: WatchlistCapacityEpoch,
    incoming: &[WatchlistEntry],
    incoming_last_trade: &HashMap<WalletAddress, i64>,
) -> Result<(usize, Vec<WalletAddress>), MembershipApplyError> {
    let _guard = writer_lock.lock().await;
    let applied = applied_capacity.load();
    if applied != expected_capacity {
        return Err(stale_capacity_error(expected_capacity, applied));
    }
    apply_ranked_membership_locked(
        live,
        paper_state,
        incoming,
        incoming_last_trade,
        expected_capacity.target,
    )
}

/// Cross-tick ranking-batch memory.
///
/// `marker` is the batch whose rows were last applied (or, in knockout mode, last observed).
/// `capacity_generation` is the capacity epoch full-rerank membership was last synced under: a
/// capacity transition publishes rows from its own `latest_ranking` read, which can predate the
/// batch this loop last applied, so the next full-rerank tick re-applies the newest batch even
/// when the marker already names it (#542). The marker itself is never erased — it still decides
/// whether a tick is a genuine batch transition, which is what clears the eviction memory.
struct BatchSync {
    marker: Option<i64>,
    capacity_generation: u64,
}

/// Run the maintenance tick loop until the process exits.
///
/// `cfg.interval_secs == 0` disables the tick (returns immediately). The first tick fires one
/// interval after startup, so the poller has advanced each bootstrap-seeded cursor forward from
/// its real last trade (#357) before the first inactivity check — there is no admission grace.
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
    // Seeded with the batch observed at boot (fetched BEFORE the boot watchlist so a batch
    // landing in between reads as a transition, never as already-seen — review finding on
    // the first draft): a batch pushed between boot and the first tick now swaps on that
    // first tick instead of being pinned as current and skipped until the next batch. A `None`
    // marker (the boot batch read failed) is an ordinary transition too (#542): the first tick
    // applies the batch it triggers on rather than adopting the identifier without applying it.
    let mut sync = BatchSync {
        marker: initial_batch_marker,
        capacity_generation: applied_capacity.load().generation,
    };
    loop {
        tokio::time::sleep(interval).await;
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
    }
}

/// Load per-wallet edge stats from the authoritative local paper-state. `None` (with a
/// warn) on any read failure — the knockout pass skips its tick; the full-rerank audit
/// degrades to stat-less rows.
fn load_edge_stats(
    paper_state: &Arc<PaperStateDb>,
    cfg: &MaintenanceConfig,
    now_unix: i64,
) -> Option<HashMap<String, WalletEdgeStats>> {
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
    Some(wallet_edge_stats(
        &fills,
        &resolutions,
        cfg.demotion_cb_alpha,
        now_unix,
        cfg.demotion_pnl_window_secs,
    ))
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
    let cap = capacity_epoch.target;
    // A capacity transition since the last sync means membership may reflect an older
    // `latest_ranking` read than the batch this loop last applied (#542).
    let capacity_changed = capacity_epoch.generation != sync.capacity_generation;

    // 1. Ranking-batch step — UNCONDITIONAL, never coupled to local DB read health (the
    // legacy tick ran it first; a review finding on the first draft caught the reorder).
    // Knockout mode: a fresh batch only clears the evicted-set so re-promoted wallets can
    // return. FullRerank mode: a batch TRANSITION hands membership to the ranker —
    // wholesale swap to the new top-`cap`; the marker only advances on a successful swap
    // so a failed fetch retries next tick. Audit stats for dropped wallets are best-effort
    // decoration: a list_fills failure degrades the audit rows, never blocks the swap.
    match supabase_reader::fetch_latest_batch_id(client, base_url, anon_key, secret_key).await {
        Ok(latest) => match cfg.membership_mode {
            MembershipMode::Knockout => {
                if latest.is_some() && latest != sync.marker {
                    if sync.marker.is_some() {
                        evicted.clear();
                    }
                    sync.marker = latest;
                }
                // Knockout membership is never batch-applied, so there is nothing to re-sync.
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
                    match supabase_reader::fetch_batch(
                        client, base_url, anon_key, secret_key, batch_id, cap,
                    )
                    .await
                    {
                        Ok((incoming, incoming_last_trade)) => {
                            let (_, additions) = ranked_membership_change(
                                &live.snapshot().entries,
                                &incoming.entries,
                                cap,
                            );
                            if let Err(error) = preparer.prepare(&additions).await {
                                warn!(%error, batch_id,
                                    "full_rerank: admission preparation failed; keeping membership and batch marker for retry");
                                return;
                            }
                            let (live_total, dropped) = match apply_full_rerank_swap(
                                live,
                                paper_state,
                                writer_lock,
                                applied_capacity,
                                capacity_epoch,
                                &incoming.entries,
                                &incoming_last_trade,
                            )
                            .await
                            {
                                Ok(applied) => applied,
                                Err(error) => {
                                    warn!(%error,
                                        "full_rerank: structural apply failed; keeping membership and batch marker for retry");
                                    return;
                                }
                            };
                            let audit_stats = load_edge_stats(paper_state, cfg, now_unix);
                            for w in &dropped {
                                let s = audit_stats.as_ref().and_then(|m| m.get(&w.to_string()));
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
                            return;
                        }
                        Err(e) => warn!(error = %e, batch_id,
                            "full_rerank: pinned batch fetch failed; keeping membership, will retry next tick"),
                    }
                }
            }
        },
        Err(e) => warn!(error = %e, "maintenance: batch-id fetch failed; keeping evicted-set"),
    }

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
    let evictions = decide_evictions(&live_snapshot, &stats, &cursors, cfg, now_unix);
    if evictions.is_empty() && live_wallets.len() >= cap {
        return;
    }

    // 5. Stage this tick's evictions. Commit the cross-tick memory only after the structural
    // write succeeds; a stale capacity epoch must leave both membership and policy memory intact.
    let mut next_evicted = evicted.clone();
    for ev in &evictions {
        next_evicted.insert(ev.wallet);
    }

    // 6. Fetch bench candidates for freed slots, excluding (live ∪ evicted), then atomic replace.
    let survivors = live_wallets.len().saturating_sub(evictions.len());
    let freed = cap.saturating_sub(survivors);
    let (candidates, candidate_last_trade) = if freed > 0 {
        let exclude: Vec<WalletAddress> = live_wallets
            .iter()
            .copied()
            .chain(next_evicted.iter().copied())
            .collect();
        match supabase_reader::fetch_candidates(
            client,
            base_url,
            anon_key,
            secret_key,
            &exclude,
            freed + cfg.bench_overfetch,
            now_unix,
        )
        .await
        {
            // The candidate last-trade side-map (#357) seeds each admitted wallet's poll cursor
            // (its inactivity clock) from the wallet's real last trade in the apply step.
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
                (w.entries, candidate_last_trade)
            }
            Err(e) => {
                warn!(error = %e, "maintenance: candidate fetch failed; evicting without backfill");
                (Vec::new(), HashMap::new())
            }
        }
    } else {
        (Vec::new(), HashMap::new())
    };

    // The freed slots are filled from `candidates` by `planned_admissions`, which is a pure
    // function of the same inputs the writer-locked apply re-reads under the unchanged capacity
    // epoch. Prepare exactly those wallets first (#542); on failure apply the decided evictions
    // with no backfill and let the next tick retry the freed slots.
    let removed: HashSet<WalletAddress> = next_evicted.iter().copied().collect();
    let planned = planned_admissions(&live_snapshot.entries, &removed, &candidates, cap);
    let (candidates, candidate_last_trade) = match preparer.prepare(&planned).await {
        Ok(()) => (candidates, candidate_last_trade),
        Err(error) => {
            warn!(%error,
                "maintenance: admission preparation failed; evicting without backfill");
            (Vec::new(), HashMap::new())
        }
    };

    let live_total = match apply_evictions_and_backfill(
        live,
        paper_state,
        writer_lock,
        applied_capacity,
        capacity_epoch,
        &removed,
        &candidates,
        &candidate_last_trade,
    )
    .await
    {
        Ok(total) => total,
        Err(error) => {
            warn!(%error,
                "maintenance: structural apply failed; keeping membership and eviction memory");
            return;
        }
    };
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
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn cfg() -> MaintenanceConfig {
        MaintenanceConfig {
            interval_secs: 600,
            inactivity_threshold_secs: 259_200, // 72 h
            inactivity_hard_cap_secs: 604_800,  // 7 d
            demotion_min_trades: 10,
            demotion_cb_alpha: dec!(0.10),
            demotion_pnl_window_secs: 2_592_000, // 30 d
            bench_overfetch: 10,
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
        use std::sync::atomic::{AtomicUsize, Ordering};

        use axum::extract::{Query, State};
        use axum::http::StatusCode;
        use axum::{Json, Router, routing::get};
        use pe_core_types::{BasisPoints, ReconstructionQuality, SourceTimestamp};
        use pe_trader_index::WatchlistTier;
        use tempfile::TempDir;
        use tokio::sync::mpsc;

        use super::*;
        use crate::orchestrator_control::OrchestratorControl;

        const CAP: usize = 3;

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
        /// query the pinned read sends; `latest_ranking` serves the knockout bench.
        #[derive(Clone)]
        struct Fake {
            latest_batch: Option<i64>,
            ranking_entries: Vec<serde_json::Value>,
            latest_ranking: Vec<serde_json::Value>,
            history_ok: bool,
            activity_hits: Arc<AtomicUsize>,
            position_hits: Arc<AtomicUsize>,
        }

        impl Fake {
            fn new(latest_batch: Option<i64>) -> Self {
                Self {
                    latest_batch,
                    ranking_entries: Vec::new(),
                    latest_ranking: Vec::new(),
                    history_ok: true,
                    activity_hits: Arc::new(AtomicUsize::new(0)),
                    position_hits: Arc::new(AtomicUsize::new(0)),
                }
            }

            async fn serve(self) -> String {
                async fn batches(State(fake): State<Fake>) -> Json<serde_json::Value> {
                    Json(match fake.latest_batch {
                        Some(id) => serde_json::json!([{ "batch_id": id }]),
                        None => serde_json::json!([]),
                    })
                }
                async fn entries(
                    State(fake): State<Fake>,
                    Query(q): Query<HashMap<String, String>>,
                ) -> Json<Vec<serde_json::Value>> {
                    let pinned = q
                        .get("batch_id")
                        .and_then(|v| v.strip_prefix("eq."))
                        .and_then(|v| v.parse::<i64>().ok())
                        .expect("pinned read must carry batch_id=eq.N");
                    Json(
                        fake.ranking_entries
                            .iter()
                            .filter(|r| r["batch_id"].as_i64() == Some(pinned))
                            .cloned()
                            .collect(),
                    )
                }
                async fn latest(State(fake): State<Fake>) -> Json<Vec<serde_json::Value>> {
                    Json(fake.latest_ranking.clone())
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
            paper_state: Arc<PaperStateDb>,
            preparer: crate::watchlist_admission::AdmissionPreparer,
            applied: AppliedWatchlistCapacity,
            writer_lock: Mutex<()>,
            client: reqwest::Client,
            base_url: String,
            controls: Arc<StdMutex<ControlLog>>,
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
                .filter(|wallet| fake.history_ok || initial.contains(wallet))
                .collect();
            let base_url = fake.serve().await;
            let temp = TempDir::new().unwrap();
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
            let live = live(initial);
            let (control_tx, mut control_rx) = mpsc::channel(2);
            let controls: Arc<StdMutex<ControlLog>> = Arc::new(StdMutex::new(Vec::new()));
            let (control_live, control_log) = (live.clone(), Arc::clone(&controls));
            let fake_paper_state = Arc::clone(&paper_state);
            std::mem::drop(tokio::spawn(async move {
                while let Some(message) = control_rx.recv().await {
                    match message {
                        OrchestratorControl::PrepareAdmissions {
                            wallets,
                            acknowledged,
                        } => {
                            // Mirror the real orchestrator's successful acceptance: a
                            // prepared wallet gains a current causal position validation,
                            // or the publication recheck would (correctly) reject it. The
                            // bracket itself is proven in scenario_position_bracket.rs.
                            let validations: Vec<pe_paper_state::PositionValidationRecord> =
                                wallets
                                    .iter()
                                    .map(|wallet| pe_paper_state::PositionValidationRecord {
                                        wallet: *wallet,
                                        ledger_hash: "test-ledger".to_owned(),
                                        positions_proof_hash: "test-proof".to_owned(),
                                        activity_bounds_json: "{}".to_owned(),
                                        source_log_generation: "test-gen".to_owned(),
                                        proof_json: "{}".to_owned(),
                                        recorded_at_unix: 0,
                                    })
                                    .collect();
                            fake_paper_state
                                .record_position_validations(&validations)
                                .unwrap();
                            control_log
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner)
                                .push((wallets.into_iter().collect(), members(&control_live)));
                            acknowledged.send(()).unwrap();
                        }
                        OrchestratorControl::CommitActivityBucket { .. } => {
                            panic!("maintenance sent an activity bucket")
                        }
                        OrchestratorControl::InstallAnchors { .. }
                        | OrchestratorControl::CaptureAdmissionLedger { .. } => {
                            panic!("legacy admission test sent a causal-bracket command")
                        }
                    }
                }
            }));
            let preparer = crate::watchlist_admission::AdmissionPreparer::new(
                control_tx,
                Arc::clone(&paper_state),
            );
            Harness {
                live,
                paper_state,
                preparer,
                applied: AppliedWatchlistCapacity::new(CAP),
                writer_lock: Mutex::new(()),
                client: reqwest::Client::new(),
                base_url,
                controls,
                _temp: temp,
            }
        }

        impl Harness {
            async fn tick(
                &self,
                mode: MembershipMode,
                evicted: &mut HashSet<WalletAddress>,
                marker: &mut Option<i64>,
            ) {
                let mut sync = BatchSync {
                    marker: *marker,
                    capacity_generation: self.applied.load().generation,
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

        fn set(wallets: &[WalletAddress]) -> HashSet<WalletAddress> {
            wallets.iter().copied().collect()
        }

        #[tokio::test]
        async fn full_rerank_prepares_exact_additions_before_publishing() {
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
        async fn full_rerank_preparation_failure_keeps_membership_and_marker() {
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
            assert_eq!(
                members(&h.live),
                set(&[a]),
                "unprepared wallet was published"
            );
            assert_eq!(marker, Some(1), "marker advanced past an unapplied batch");
            assert_eq!(position_hits.load(Ordering::SeqCst), 0);
            assert_eq!(h.paper_state.cursor(&b).unwrap(), None);
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
            fake.ranking_entries = vec![row(2, 1, b), row(3, 1, c)];
            fake.latest_ranking = vec![row(3, 1, c)];
            let h = harness(fake, &[a]).await;
            let (mut evicted, mut marker) = (HashSet::new(), Some(1));

            h.tick(MembershipMode::FullRerank, &mut evicted, &mut marker)
                .await;

            assert_eq!(h.controls(), vec![(set(&[b]), set(&[a]))]);
            assert_eq!(members(&h.live), set(&[b]));
            assert_eq!(marker, Some(2));
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
                marker: Some(2),
                capacity_generation: 0,
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
                marker: Some(1),
                capacity_generation: 0,
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
    }
}
