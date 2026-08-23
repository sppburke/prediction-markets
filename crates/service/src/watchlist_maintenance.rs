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
//!   safety rail — a readmitted bleeder is re-demotable on the next tick. A failed top fetch
//!   leaves the batch marker unadvanced so the swap retries next tick; the boot-observed batch
//!   never triggers a swap (boot already seeded exactly that batch's top-`cap`).

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
    let current = live.snapshot();
    let admissions = planned_admissions(
        &current.entries,
        removed,
        candidates,
        expected_capacity.target,
    );
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
    let current = live.snapshot();
    let incoming_set: HashSet<WalletAddress> = incoming
        .iter()
        .take(cap)
        .map(|entry| entry.wallet)
        .collect();
    let dropped: Vec<WalletAddress> = current
        .entries
        .iter()
        .map(|entry| entry.wallet)
        .filter(|wallet| !incoming_set.contains(wallet))
        .collect();
    let removed: HashSet<WalletAddress> = dropped.iter().copied().collect();
    let admissions = planned_admissions(&current.entries, &removed, incoming, cap);
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
    // first tick instead of being pinned as current and skipped until the next batch.
    let mut batch_marker: Option<i64> = initial_batch_marker;
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
            &cfg,
            capacity_epoch,
            &mut evicted,
            &mut batch_marker,
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
    cfg: &MaintenanceConfig,
    capacity_epoch: WatchlistCapacityEpoch,
    evicted: &mut HashSet<WalletAddress>,
    batch_marker: &mut Option<i64>,
) {
    let now_unix = OffsetDateTime::now_utc().unix_timestamp();
    let cap = capacity_epoch.target;

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
                if latest.is_some() && latest != *batch_marker {
                    if batch_marker.is_some() {
                        evicted.clear();
                    }
                    *batch_marker = latest;
                }
            }
            MembershipMode::FullRerank => {
                if latest.is_some() && batch_marker.is_none() {
                    // Boot alignment: startup already seeded exactly this batch's top-cap.
                    *batch_marker = latest;
                } else if latest.is_some() && latest != *batch_marker {
                    match supabase_reader::fetch(client, base_url, anon_key, secret_key, cap).await
                    {
                        Ok((incoming, incoming_last_trade)) if !incoming.entries.is_empty() => {
                            let swap = apply_full_rerank_swap(
                                live,
                                paper_state,
                                writer_lock,
                                applied_capacity,
                                capacity_epoch,
                                &incoming.entries,
                                &incoming_last_trade,
                            )
                            .await;
                            let (live_total, dropped) = match swap {
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
                            // memory at each batch; the knockout resumes next tick.
                            evicted.clear();
                            *batch_marker = latest;
                            info!(
                                batch_id = latest.unwrap_or(-1),
                                dropped = dropped.len(),
                                live_total,
                                "full re-rank membership swap applied"
                            );
                            return;
                        }
                        // #518 made this branch reachable in normal operation: the read is
                        // survivor-filtered, so a batch whose rows all fail the gate — or one
                        // that carries no verdict at all — legitimately returns zero rows.
                        // Retaining the previous set here would keep copying wallets the CURRENT
                        // batch says are ineligible, and would do so forever, because the marker
                        // never advances. Apply the empty membership instead: it matches the
                        // cold-boot stance (`main.rs` refuses to start on an empty filtered
                        // read) and the fail-closed contract — the ranker endorsing nobody means
                        // copying nobody. Open positions keep resolving; only new copies stop.
                        Ok((incoming, incoming_last_trade)) => {
                            match apply_full_rerank_swap(
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
                                Ok((live_total, dropped)) => {
                                    evicted.clear();
                                    *batch_marker = latest;
                                    warn!(
                                        batch_id = latest.unwrap_or(-1),
                                        dropped = dropped.len(),
                                        live_total,
                                        "full_rerank: batch has no surviving rows; live set \
                                         emptied (fail-closed — the ranker endorsed nobody)"
                                    );
                                }
                                Err(e) => warn!(error = %e,
                                    "full_rerank: empty-batch swap rejected; keeping membership, \
                                     will retry next tick"),
                            }
                            return;
                        }
                        Err(e) => warn!(error = %e,
                            "full_rerank: top fetch failed; keeping membership, will retry next tick"),
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

    let removed: HashSet<WalletAddress> = next_evicted.iter().copied().collect();
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
}
