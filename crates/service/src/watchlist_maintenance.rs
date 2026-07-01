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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use pe_core_types::WalletAddress;
use pe_paper_pnl::ResolutionStore;
use pe_paper_state::PaperStateDb;
use pe_trader_index::{Watchlist, WatchlistEntry};
use rust_decimal::Decimal;
use time::OffsetDateTime;
use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::demotion_stat::{WalletEdgeStats, wallet_edge_stats};
use crate::live_watchlist::LiveWatchlist;
use crate::supabase_reader;

/// Tuning for the maintenance tick. Every field is sourced from [`crate::config::ServiceConfig`]
/// (defaults registered in `docs/_GLOSSARY.md`); `cap` is `supabase_reader::MAINTAINED_SET_SIZE`.
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
    /// Working-set size cap (the maintained-N).
    pub cap: usize,
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
}

impl KnockoutReason {
    /// Audit text written to `wallet_lifecycle_events.reason`.
    #[must_use]
    pub fn reason_text(self) -> &'static str {
        match self {
            Self::Inactivity => "inactive>72h",
            Self::InactivityHardCap => "inactive>7d (hard cap)",
            Self::Underperformance => "upper_cb_edge<0 & windowed_pnl<0",
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

/// Apply the decided evictions and backfill freed slots, atomically, under the writer lock.
///
/// Inside the single writer-locked critical section: snapshot the live set, [`LiveWatchlist::replace`]
/// (evict `removed`, backfill `candidates`, cap to `cap`), then seed the poll cursor of each
/// *newly admitted* wallet from its real last-trade time in `candidate_last_trade` (#357), so its
/// inactivity clock starts at the real last trade — before the poller's next round advances it
/// forward. The poller snapshots the live set once per round, so a wallet admitted here is invisible
/// to the in-flight round and this seed always lands first. A candidate absent from
/// `candidate_last_trade` (should not happen: the freshness filter requires a non-NULL
/// `last_trade_unix`) falls back to `now_unix`. Returns the new live-set size.
#[allow(clippy::too_many_arguments)]
pub async fn apply_evictions_and_backfill(
    live: &LiveWatchlist,
    paper_state: &PaperStateDb,
    writer_lock: &Mutex<()>,
    removed: &HashSet<WalletAddress>,
    candidates: &[WatchlistEntry],
    candidate_last_trade: &HashMap<WalletAddress, i64>,
    cap: usize,
    now_unix: i64,
) -> usize {
    let _guard = writer_lock.lock().await;
    let before: HashSet<WalletAddress> = live.snapshot().entries.iter().map(|e| e.wallet).collect();
    let total = live.replace(removed, candidates, cap);
    for entry in &live.snapshot().entries {
        if !before.contains(&entry.wallet) {
            // Newly admitted via backfill: seed the inactivity clock from the wallet's real
            // last-trade time (#357), not `now`. `now_unix` is only a defensive fallback —
            // a freshness-filtered candidate always carries a `last_trade_unix`.
            let seed = candidate_last_trade
                .get(&entry.wallet)
                .copied()
                .unwrap_or(now_unix);
            if let Err(e) = paper_state.set_cursor(&entry.wallet, seed) {
                warn!(wallet = %entry.wallet, error = %e, "failed to seed backfill cursor");
            }
        }
    }
    total
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
    let mut batch_marker: Option<i64> = None;
    loop {
        tokio::time::sleep(interval).await;
        maintenance_tick(
            &live,
            &paper_state,
            &client,
            &base_url,
            &anon_key,
            &secret_key,
            &writer_lock,
            &cfg,
            &mut evicted,
            &mut batch_marker,
        )
        .await;
    }
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
    cfg: &MaintenanceConfig,
    evicted: &mut HashSet<WalletAddress>,
    batch_marker: &mut Option<i64>,
) {
    let now_unix = OffsetDateTime::now_utc().unix_timestamp();

    // 1. Detect a fresh ranking batch → clear the evicted-set so re-promoted wallets can return.
    match supabase_reader::fetch_latest_batch_id(client, base_url, anon_key, secret_key).await {
        Ok(latest) => {
            if latest.is_some() && latest != *batch_marker {
                if batch_marker.is_some() {
                    evicted.clear();
                }
                *batch_marker = latest;
            }
        }
        Err(e) => warn!(error = %e, "maintenance: batch-id fetch failed; keeping evicted-set"),
    }

    // 2. Per-wallet edge stats from the authoritative local paper-state.
    let fills = match paper_state.list_fills() {
        Ok(f) => f,
        Err(e) => {
            warn!(error = %e, "maintenance: list_fills failed; skipping tick");
            return;
        }
    };
    let resolutions = match ResolutionStore::load(Arc::clone(paper_state)) {
        Ok(r) => r,
        Err(e) => {
            warn!(error = %e, "maintenance: resolution-store load failed; skipping tick");
            return;
        }
    };
    let stats = wallet_edge_stats(
        &fills,
        &resolutions,
        cfg.demotion_cb_alpha,
        now_unix,
        cfg.demotion_pnl_window_secs,
    );

    // 3. Snapshot the live wallets and their cursors.
    let live_snapshot = live.snapshot();
    let live_wallets: Vec<WalletAddress> = live_snapshot.entries.iter().map(|e| e.wallet).collect();
    let mut cursors: HashMap<WalletAddress, Option<i64>> =
        HashMap::with_capacity(live_wallets.len());
    for w in &live_wallets {
        // A cursor read error self-heals to `None` (treated as just-admitted, not inactive).
        cursors.insert(*w, paper_state.cursor(w).unwrap_or(None));
    }

    // 4. Decide evictions. Nothing to do only when there are no evictions and the set is full.
    let evictions = decide_evictions(&live_snapshot, &stats, &cursors, cfg, now_unix);
    if evictions.is_empty() && live_wallets.len() >= cfg.cap {
        return;
    }

    // 5. Record this tick's evictions in the cross-tick evicted-set.
    for ev in &evictions {
        evicted.insert(ev.wallet);
    }

    // 6. Fetch bench candidates for freed slots, excluding (live ∪ evicted), then atomic replace.
    let survivors = live_wallets.len().saturating_sub(evictions.len());
    let freed = cfg.cap.saturating_sub(survivors);
    let (candidates, candidate_last_trade) = if freed > 0 {
        let exclude: Vec<WalletAddress> = live_wallets
            .iter()
            .copied()
            .chain(evicted.iter().copied())
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
                    warn!(
                        "maintenance: freshness-filtered candidate fetch returned 0; backfill \
                         paused (bench may predate the last_trade_unix populate push)"
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

    let removed: HashSet<WalletAddress> = evicted.iter().copied().collect();
    let live_total = apply_evictions_and_backfill(
        live,
        paper_state,
        writer_lock,
        &removed,
        &candidates,
        &candidate_last_trade,
        cfg.cap,
        now_unix,
    )
    .await;

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
            cap: 25,
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
