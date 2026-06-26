//! Phase 1: walk-forward simulation.
//!
//! Walk-forward invariant (enforced): at simulated date D, only trades with
//! `timestamp.date() < D` are visible to the ranker. Trades with `timestamp.date() == D`
//! are the new signals; the ranker was built from strictly prior data.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::Write as _;
use std::path::Path;

use pe_bootstrap::cache::{
    ClobMarkIndex, LeaderboardSnapshots, LiquidityIndex, MarketResolution, ResolutionIndex,
    ScheduleIndex, WalletCache,
};
use pe_copy_signal_engine::LeaderSignal;
use pe_core_types::{
    BasisPoints, KellyFraction, LeaderAction, MarketId, OutcomeId, Probability, ProbabilityPpm,
    Quantity, ReconstructionQuality, Side, SourceTimestamp, TraderId, VenueId, WalletAddress,
};
use pe_risk_engine::RiskSnapshot;
use pe_risk_engine::clamp_contracts_to_liquidity;
use pe_risk_engine::snapshot::TradingMode;
use pe_source_core::SourceStatus;
use pe_strategy_winner_follow::{WinnerFollowConfig, WinnerFollowStrategy};
use pe_trader_index::ledger::TraderLedger;
use pe_trader_index::snapshot::RawTrade;
use pe_trader_index::{IncrementalLedger, RankerConfig, build_watchlist};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive as _;
use serde::Serialize;
use serde::ser::Error as _;
use time::{Date, OffsetDateTime};
use tracing::info;

use crate::config::BacktestConfig;
use crate::error::BacktestError;
use crate::report::{
    KellySweepRun, PnlAccumulator, TradeFill, WinnerFollowReport, max_drawdown_pct, sharpe_ratio,
};

// Warn threshold for per-quarter BUY-signal suppression — shared by every
// labeled `SuppressionTracker` instance (expiry filter, high-price cap, …).
// Canonical: `backtest_suppression_warn_threshold_pct = 30` in `docs/_GLOSSARY.md`.
const SUPPRESSION_WARN_THRESHOLD: u32 = 30;

/// Per-quarter statistics for a BUY-signal suppression diagnostic.
#[derive(Debug, Default)]
struct QuarterStats {
    total: u64,
    suppressed: u64,
}

/// Tracks how many BUY signals a gate suppresses, broken down by calendar
/// quarter. Emits a warning when any quarter exceeds the canonical threshold.
/// The `label` distinguishes diagnostics in the warning stream (e.g. an
/// `"expiry filter"` instance vs a `"high-price cap"` instance).
#[derive(Debug)]
struct SuppressionTracker {
    label: &'static str,
    by_quarter: BTreeMap<(i32, u8), QuarterStats>,
}

impl SuppressionTracker {
    fn new(label: &'static str) -> Self {
        Self {
            label,
            by_quarter: BTreeMap::new(),
        }
    }

    fn record(&mut self, date: Date, suppressed: bool) {
        let quarter = (date.month() as u8 - 1) / 3 + 1;
        let stats = self.by_quarter.entry((date.year(), quarter)).or_default();
        stats.total += 1;
        if suppressed {
            stats.suppressed += 1;
        }
    }

    fn suppression_pct_global(&self) -> Decimal {
        let total: u64 = self.by_quarter.values().map(|s| s.total).sum();
        let suppressed: u64 = self.by_quarter.values().map(|s| s.suppressed).sum();
        if total == 0 {
            return Decimal::ZERO;
        }
        Decimal::from(suppressed * 100) / Decimal::from(total)
    }

    fn per_quarter_suppression(&self) -> std::collections::BTreeMap<String, Decimal> {
        self.by_quarter
            .iter()
            .map(|((year, q), stats)| {
                let key = format!("{year}-Q{q}");
                let pct = if stats.total == 0 {
                    Decimal::ZERO
                } else {
                    Decimal::from(stats.suppressed * 100) / Decimal::from(stats.total)
                };
                (key, pct)
            })
            .collect()
    }

    fn warn_high_quarters(&self, threshold_pct: u32) {
        let threshold = Decimal::from(threshold_pct);
        for ((year, q), stats) in &self.by_quarter {
            if stats.total == 0 {
                continue;
            }
            let pct = Decimal::from(stats.suppressed * 100) / Decimal::from(stats.total);
            if pct > threshold {
                tracing::warn!(
                    quarter = %format!("{year}-Q{q}"),
                    suppression_pct = %pct,
                    threshold_pct,
                    label = self.label,
                    "backtest: suppression threshold exceeded"
                );
            }
        }
    }
}

/// Open position entry (copies we've taken but not yet closed).
#[derive(Debug, Clone)]
struct OpenPosition {
    contracts: u64,
    avg_fill_price: Decimal,
    /// Calendar date on which the copy was opened. Used by the resolution sweep to
    /// guard against anomalies where a market's resolved_at precedes the bought_on date.
    bought_on: Date,
    /// Exact open timestamp (unix seconds), used by the forward MTM (issue #436
    /// Phase E) to decide whether a position was open at a window boundary.
    open_unix: i64,
}

/// Position key: (market, outcome, BUY side).
type PosKey = (MarketId, OutcomeId);

/// A closed-or-still-open copy position recorded for the forward mark-to-market
/// (issue #436 Phase E). Only accumulated on the injected-set path when an MTM
/// window is configured; lets the post-loop pass decide whether the position was
/// open at the window's `as_of` start and/or `horizon` end (`open_unix <= T <
/// close_unix`) and mark it at each boundary via the [`ClobMarkIndex`].
#[derive(Debug, Clone)]
struct PositionLifetime {
    wallet: WalletAddress,
    market: MarketId,
    outcome: OutcomeId,
    contracts: u64,
    avg_fill_price: Decimal,
    open_unix: i64,
    /// `None` = still open at simulation end (never closed by sell or resolution).
    close_unix: Option<i64>,
}

impl PositionLifetime {
    /// True when the position was open at instant `t` (opened at-or-before `t`,
    /// not yet closed at `t`). Strict `close_unix > t` so a position closing
    /// exactly at the boundary is treated as closed there.
    fn open_at(&self, t: i64) -> bool {
        self.open_unix <= t && self.close_unix.is_none_or(|c| c > t)
    }
}

/// Exposure tracker: per-leader, per-market.
#[derive(Debug, Default)]
struct ExposureTracker {
    /// Open exposure in bps of bankroll per leader.
    by_leader: HashMap<WalletAddress, i32>,
    /// Open exposure in bps of bankroll per market.
    by_market: HashMap<MarketId, i32>,
    /// Count of concurrent open positions per market (issue #138). Used by the
    /// per-market position cap gate in the BUY arm. Incremented once per `add`
    /// regardless of bps, decremented once per `remove` (saturating at 0).
    by_market_count: HashMap<MarketId, u32>,
    /// Total copy exposure across all positions.
    total: i32,
}

impl ExposureTracker {
    fn leader_bps(&self, w: WalletAddress) -> i32 {
        self.by_leader.get(&w).copied().unwrap_or(0)
    }

    fn market_bps(&self, m: &MarketId) -> i32 {
        self.by_market.get(m).copied().unwrap_or(0)
    }

    /// Concurrent open-position count for a market (issue #138).
    ///
    /// Returns 0 when the market has never been opened or all positions have closed.
    /// Used by the per-market position cap gate before any sizing branch.
    fn market_position_count(&self, m: &MarketId) -> u32 {
        self.by_market_count.get(m).copied().unwrap_or(0)
    }

    /// Increment exposure counters when a new position opens.
    ///
    /// Split from `remove` because `by_market_count` is non-linear (each open
    /// adds 1 regardless of `bps`); a `remove → add(-bps)` delegation would
    /// incorrectly increment the counter on close.
    fn add(&mut self, w: WalletAddress, m: &MarketId, bps: i32) {
        *self.by_leader.entry(w).or_default() += bps;
        *self.by_market.entry(m.clone()).or_default() += bps;
        *self.by_market_count.entry(m.clone()).or_default() += 1;
        self.total += bps;
    }

    /// Decrement exposure counters when an existing position closes.
    ///
    /// Mirror of `add`. `by_market_count` decrements with saturation at 0 to
    /// guard against a hypothetical double-close that callers should never
    /// trigger but which a panic would convert into a fatal error.
    fn remove(&mut self, w: WalletAddress, m: &MarketId, bps: i32) {
        *self.by_leader.entry(w).or_default() -= bps;
        *self.by_market.entry(m.clone()).or_default() -= bps;
        if let Some(n) = self.by_market_count.get_mut(m) {
            *n = n.saturating_sub(1);
        }
        self.total -= bps;
    }
}

/// Run the walk-forward simulation and produce a `WinnerFollowReport`.
///
/// `snapshots` constrains the candidate-wallet pool at each weekly boundary to
/// match what the live system would have seen at that point in time. When
/// [`LeaderboardSnapshots::is_empty`] is true the simulation falls back to
/// "all wallets present in the trade history" with a single warning log —
/// preserves backwards compatibility with caches predating the snapshots feature.
///
/// `resolutions` drives the per-day resolution sweep that closes open positions
/// whose underlying market settled on-chain. Pass `&ResolutionIndex::new()` when
/// no resolution data is available (sweep is a no-op; positions remain open at horizon).
///
/// `schedules` provides scheduled `endDate` values for the `max_hours_to_expiry` buy
/// filter. When a market is present in `schedules`, its `end_date_unix` is used (fixing
/// the survivorship bias introduced by using `resolved_at_unix` as a proxy). When absent,
/// the filter falls back to `resolved_at_unix`. Pass `&ScheduleIndex::new()` to use the
/// legacy resolution-only path.
///
/// Writes `report.json` and `trades.ndjson` to `config.output_dir` when `write_output` is true.
///
/// Pass `write_output = false` from sweep mode — per-run files are suppressed and only
/// the sweep-level JSON is written by the caller.
///
/// # Precondition
///
/// `all_trades` must be sorted ascending by `t.timestamp.0`. The walk-forward
/// loop and `simulation_start`/`simulation_end` derivation both assume this
/// ordering. The caller is responsible for sorting once before invoking;
/// `main.rs` does this above the sweep/non-sweep branch so a single sort
/// covers both paths and the slice can be borrowed across rayon workers.
/// Violating the precondition fires `debug_assert!` in debug builds; release
/// builds will produce incorrect results without panicking.
///
/// # Injected-set mode (#421 bake-off)
///
/// When `injected` is `Some(set)`, the ranker (`build_watchlist`) and the
/// leaderboard-snapshot machinery are bypassed: the followed watchlist IS
/// `set`, every step. `all_trades` must then contain only those wallets'
/// trades (the caller bounded-loads via [`load_injected_trades`]). The copy
/// path is the `flat_usd` short-circuit, which reads only set membership, so
/// the (empty) ledger/quality/snapshot maps are never consulted — callers MUST
/// set `config.flat_usd` (enforced here and in `main`). On this path the run also emits
/// `pnl_by_period.ndjson` (per-period, per-wallet copy P&L) when `write_output`.
///
/// This is the ranker-or-injected entry point. The 9-argument [`run_simulation`]
/// wrapper forwards `injected = None` for the (unchanged) ranker-only callers.
#[allow(clippy::too_many_arguments)]
pub fn run_simulation_with(
    config: &BacktestConfig,
    all_trades: &[RawTrade],
    snapshots: &LeaderboardSnapshots,
    resolutions: &ResolutionIndex,
    schedules: &ScheduleIndex,
    liq_index: &LiquidityIndex,
    ranker_config: &RankerConfig,
    strategy: &WinnerFollowStrategy,
    write_output: bool,
    injected: Option<&HashSet<WalletAddress>>,
    clob_marks: Option<&ClobMarkIndex>,
) -> Result<WinnerFollowReport, BacktestError> {
    debug_assert!(
        all_trades.is_sorted_by_key(|t| t.timestamp.0),
        "run_simulation precondition violated: all_trades must be sorted by t.timestamp.0",
    );

    if all_trades.is_empty() {
        return Err(BacktestError::Internal("no trades in cache".to_owned()));
    }

    // Injected-set mode copies via the flat-USD short-circuit, which is the only
    // copy path that reads set membership alone (the ranker/quality/ledger maps
    // are empty here). Without `flat_usd`, the Kelly path would read the empty
    // quality map and silently drop every copy — fail loudly instead. `main`
    // also enforces this, but the guard belongs with the precondition.
    if injected.is_some() && config.flat_usd.is_none() {
        return Err(BacktestError::Internal(
            "injected-set mode requires config.flat_usd (the copy path is the flat-USD short-circuit)"
                .to_owned(),
        ));
    }

    // Backwards-compatibility fallback. Caches predating the snapshots feature
    // (or fresh caches that have never been bootstrapped) have no leaderboard
    // rows; in that mode the simulation runs with the full wallet history,
    // which is survivorship-biased. Surfaced as a single warning so it isn't
    // missed in long runs but doesn't spam per-day.
    if snapshots.is_empty() {
        tracing::warn!(
            "no leaderboard snapshots available — running with full wallet history; \
             results subject to survivorship bias. Run pe-bootstrap to populate \
             leaderboard_snapshots."
        );
    }

    // Collect unique simulation dates.
    let all_dates: Vec<Date> = {
        let mut dates: Vec<Date> = all_trades
            .iter()
            .map(|t| t.timestamp.0.date())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        dates.sort();
        dates
    };

    let simulation_start: OffsetDateTime = all_trades[0].timestamp.0;
    let simulation_end: OffsetDateTime = all_trades
        .last()
        .map(|t| t.timestamp.0)
        .unwrap_or(simulation_start);

    // Horizon cooldown — suppress new BUY opens within N days of the
    // simulation end so existing positions can close before the report writes.
    // Computed once; SELL path is always unaffected.
    let no_buy_cutoff_unix: Option<i64> = config
        .no_buy_within_horizon_days
        .map(|d| simulation_end.unix_timestamp() - i64::from(d) * 86_400);

    let slippage_rate = config.strategy.slippage_rate;
    let slippage_assumption_bps = (slippage_rate * Decimal::from(10_000u32))
        .round()
        .to_u32()
        .ok_or_else(|| {
            BacktestError::Internal(format!("slippage_rate {slippage_rate} out of u32 range"))
        })?;

    let mut bankroll = config.bankroll_usd;
    let bankroll_initial = bankroll;

    // Open positions per (wallet, market, outcome) → position entry.
    let mut open_positions: HashMap<(WalletAddress, PosKey), OpenPosition> = HashMap::new();

    let mut exposure = ExposureTracker::default();
    let mut pnl_accum = PnlAccumulator::default();
    let mut daily_bankroll: Vec<Decimal> = Vec::new();
    let mut daily_pnl: Vec<Decimal> = Vec::new();

    // Intraday and rolling 7-day PnL tracking (in bps of bankroll).
    let mut intraday_realized_pnl = Decimal::ZERO;
    let mut last_intraday_reset: Option<Date> = None;

    let mut fills_writer = maybe_open_trades_ndjson(&config.output_dir, write_output)?;

    // Per-period (UTC calendar day), per-wallet copy P&L for the #421
    // injected-set handoff (emitted as `pnl_by_period.ndjson`). Only accumulated
    // on the injected path; the full-cache ranker run leaves it empty to avoid
    // O(wallets × days) memory on the 25M-trade cache. Realized P&L is keyed to
    // the day a position closes (resolution or leader sell); `n_fills`/`notional`
    // to the day a copy opens.
    let emit_period_pnl = injected.is_some() && write_output;
    let mut period_pnl: HashMap<(WalletAddress, Date), PeriodAccum> = HashMap::new();

    // Forward mark-to-market (issue #436 Phase E): when the injected path is run
    // with both window bounds AND a CLOB mark index, value still-open positions at
    // the `(as_of, horizon]` boundaries. `mtm` is `Some((as_of, horizon, marks))`
    // only then; otherwise `unrealized_pnl` stays the documented `0.0` sentinel.
    // `mtm_lifetimes` records each copy's open/close span so the post-loop pass can
    // mark positions open at either boundary (a position that closes between
    // boundaries is gone from `open_positions`, so the live map alone is not enough).
    let mtm: Option<(i64, i64, &ClobMarkIndex)> = match (
        emit_period_pnl,
        config.mtm_window_start_unix,
        config.mtm_window_end_unix,
        clob_marks,
    ) {
        (true, Some(start), Some(end), Some(marks)) if end > start => Some((start, end, marks)),
        _ => None,
    };
    let mut mtm_lifetimes: Vec<PositionLifetime> = Vec::new();

    // Tracks leaderboard snapshot transitions for the log gate (replaces the
    // misleading `day_idx % 30` gate that fired every 30 array indices, not days).
    let mut last_logged_snapshot_unix: Option<i64> = None;

    // Tracks BUY-signal suppression from the max_hours_to_expiry filter.
    let mut suppression_tracker = SuppressionTracker::new("expiry filter");
    // Tracks BUY-signal suppression from the `max_signal_price` cap (issue #142).
    let mut high_price_tracker = SuppressionTracker::new("high-price cap");

    // Liquidity-clamp partition counters (see report.rs `liquidity_*` fields).
    // Every BUY trade reaching the clamp site increments at most one counter;
    // "data-ok passthrough" is the silent no-counter case, derivable as
    // `total_copies - fired - below_floor - unknown`.
    let mut liquidity_clamps_fired: u64 = 0;
    let mut liquidity_clamp_contracts_reduced: u64 = 0;
    let mut liquidity_below_floor_bypasses: u64 = 0;
    let mut liquidity_unknown_markets: u64 = 0;

    // Snapshot-aware prior counters (see report.rs `snapshot_prior_*` and
    // `total_signals_evaluated` fields). Denominator and numerator for the
    // activation rate `snapshot_prior_signals / total_signals_evaluated`. The
    // counter fires once per call to `leader_win_rate_p_shrunk`; in the
    // current flow that's once per `strategy.evaluate()` call. See issue #129.
    let mut total_signals_evaluated: u64 = 0;
    let mut snapshot_prior_signals: u64 = 0;
    let mut snapshot_prior_extra_sum: u64 = 0;

    // Group trades by date for efficient walk-forward lookup.
    let mut trades_by_date: BTreeMap<Date, Vec<&RawTrade>> = BTreeMap::new();
    for trade in all_trades {
        trades_by_date
            .entry(trade.timestamp.0.date())
            .or_default()
            .push(trade);
    }

    let step = config.step_days.max(1) as usize;

    let mut incr = IncrementalLedger::new();
    let mut incr_cursor = 0usize;

    for (day_idx, &sim_date) in all_dates.iter().enumerate() {
        // Reset intraday PnL at day boundary.
        if last_intraday_reset != Some(sim_date) {
            let day_pnl = intraday_realized_pnl;
            daily_pnl.push(day_pnl);
            daily_bankroll.push(bankroll);
            intraday_realized_pnl = Decimal::ZERO;
            last_intraday_reset = Some(sim_date);
        }

        // Resolution sweep: close positions whose underlying market resolved on or before sim_date.
        let sim_date_unix = sim_date.midnight().assume_utc().unix_timestamp();
        let to_close: Vec<(WalletAddress, PosKey)> = open_positions
            .iter()
            .filter_map(|((leader, pos_key), open)| {
                let res = resolutions.get(&pos_key.0)?;
                let bought_unix = open.bought_on.midnight().assume_utc().unix_timestamp();
                if res.resolved_at_unix > sim_date_unix {
                    return None;
                }
                if res.resolved_at_unix < bought_unix {
                    return None;
                }
                Some((*leader, pos_key.clone()))
            })
            .collect();

        for (leader, pos_key) in to_close {
            let Some(open) = open_positions.remove(&(leader, pos_key.clone())) else {
                continue;
            };
            let Some(res) = resolutions.get(&pos_key.0) else {
                continue;
            };
            // Realized on the sweep day (`sim_date`); the closed lifetime is recorded only when a
            // forward-MTM window is configured (`mtm.is_some()`).
            settle_position(
                leader,
                &pos_key,
                &open,
                res,
                sim_date,
                &mut bankroll,
                &mut intraday_realized_pnl,
                &mut exposure,
                &mut pnl_accum,
                &mut period_pnl,
                &mut mtm_lifetimes,
                emit_period_pnl,
                mtm.is_some(),
                fills_writer.as_mut(),
            )?;
        }

        // Advance incremental ledger: incorporate all trades strictly before today.
        // Must run on every day (including skipped steps) so state stays current.
        {
            let new_end =
                all_trades.partition_point(|t| t.timestamp.0.unix_timestamp() < sim_date_unix);
            if new_end > incr_cursor {
                incr.apply_batch(&all_trades[incr_cursor..new_end]);
                incr_cursor = new_end;
            }
        }

        // Skip days that fall outside the step window.
        if day_idx % step != 0 {
            continue;
        }

        // Resolve the followed set + supporting maps for this step. Two paths:
        //  - Injected (#421 bake-off): follow exactly `injected`, bypassing the
        //    ranker, leaderboard snapshots, and ledger build. The flat-USD copy
        //    path below reads only set membership, so the ledger / quality /
        //    snapshot maps are intentionally empty here (and never consulted).
        //  - Ranker (default): build the watchlist from the leaderboard pool.
        let filtered_ledgers: Vec<TraderLedger>;
        let watchlisted: HashSet<WalletAddress>;
        let quality_by_wallet: HashMap<WalletAddress, ReconstructionQuality>;
        let snapshots_have_data: bool;

        if let Some(inj) = injected {
            filtered_ledgers = Vec::new();
            watchlisted = inj.iter().copied().collect();
            quality_by_wallet = HashMap::new();
            snapshots_have_data = false;
        } else {
            // Skip days before the first leaderboard snapshot — filtered_ledgers
            // would be empty regardless and the trade-filter and ledger build below
            // are pure CPU waste on those days. The full-history fallback
            // (snapshots.is_empty()) keeps its prior behaviour.
            if !snapshots.is_empty() && snapshots.for_date(sim_date_unix).is_none() {
                continue;
            }

            // Resolve the leaderboard pool. When snapshots are absent (test
            // fixtures or pre-snapshot cache), build for all wallets (filter = None).
            let pool = if snapshots.is_empty() {
                None
            } else {
                match snapshots.for_date(sim_date_unix) {
                    Some(p) => Some(p),
                    // Simulation date precedes the first seeded snapshot — no candidates yet.
                    None => continue,
                }
            };

            let snapshot_at = SourceTimestamp(sim_date.midnight().assume_utc());
            filtered_ledgers = incr.build_ledgers(pool, config.audit_window_days);

            if filtered_ledgers.is_empty() {
                continue;
            }

            let watchlist = build_watchlist(&filtered_ledgers, snapshot_at.clone(), ranker_config);

            // Log on leaderboard snapshot transitions (replaces misleading day_idx % 30 gate).
            let snapshot_unix = snapshots.snapshot_at_for_date(sim_date_unix);
            if snapshot_unix != last_logged_snapshot_unix {
                info!(
                    date = %sim_date,
                    snapshot_unix,
                    filtered_ledgers = filtered_ledgers.len(),
                    active = watchlist.active_count,
                    incubator = watchlist.incubator_count,
                    "watchlist snapshot transition"
                );
                last_logged_snapshot_unix = snapshot_unix;
            }

            // Reconstruction quality map from watchlist entries.
            quality_by_wallet = watchlist
                .entries
                .iter()
                .map(|e| (e.wallet, e.reconstruction_quality))
                .collect();
            // Watchlisted wallets (Active + Incubator).
            watchlisted = watchlist.entries.iter().map(|e| e.wallet).collect();
            snapshots_have_data = !snapshots.is_empty();
        }

        // Build wallet → ledger map for O(1) win-rate lookup. Empty on the
        // injected path; the flat-USD copy path does not consult it.
        let ledger_by_wallet: HashMap<WalletAddress, &TraderLedger> =
            filtered_ledgers.iter().map(|l| (l.wallet, l)).collect();

        // Build wallet → snapshot-appearance count for the snapshot-aware prior
        // (issue #129). Pre-built per snapshot transition so the O(S) scan in
        // `snapshot_appearances_up_to` amortises across all signals in this
        // window. When snapshots are empty (test fixtures or pre-snapshots
        // caches) we have no visibility data, so the prior is disabled via the
        // `snapshots_have_data` gate at the call site — otherwise `unwrap_or(0)`
        // would feed `n_snaps = 0` to `saturating_sub`, producing the maximum
        // `extra` for every signal (the opposite of "no data → no penalty").
        let snapshot_counts: HashMap<WalletAddress, u32> = if snapshots_have_data {
            filtered_ledgers
                .iter()
                .map(|l| {
                    (
                        l.wallet,
                        snapshots.snapshot_appearances_up_to(l.wallet, sim_date_unix),
                    )
                })
                .collect()
        } else {
            HashMap::new()
        };

        // New signals: trades happening exactly on sim_date from watchlisted leaders.
        let Some(todays_trades) = trades_by_date.get(&sim_date) else {
            continue;
        };

        // Rolling 7-day PnL: sum of realized PnL for the last 7 days in bps.
        let rolling_7d_realized: Decimal = {
            let cutoff = day_idx.saturating_sub(7);
            daily_pnl.iter().skip(cutoff).sum()
        };
        // Intraday and rolling 7-day PnL in bps.
        let intraday_bps = decimal_to_bps(intraday_realized_pnl, bankroll);
        let rolling_7d_bps = decimal_to_bps(rolling_7d_realized, bankroll);

        for trade in todays_trades {
            if !watchlisted.contains(&trade.wallet) {
                continue;
            }

            let leader = trade.wallet;

            let pos_key = (trade.market_id.clone(), trade.outcome_id);
            let wallet_pos_key = (leader, pos_key.clone());

            match trade.side {
                Side::Buy => {
                    // Open a new copy position if we don't already have one for this key.
                    if open_positions.contains_key(&wallet_pos_key) {
                        continue; // Already tracking this leader's position.
                    }

                    // Per-market position cap (issue #138). Default is `Some(1)` — once
                    // any leader holds an open position on `market_id`, all subsequent
                    // BUY signals on that `market_id` (any outcome, any leader) are
                    // suppressed until the existing position closes via SELL or
                    // resolution sweep. Runs before sizing branches so flat-USD and
                    // Kelly paths honor it uniformly.
                    if let Some(cap) = config.max_positions_per_market
                        && exposure.market_position_count(&trade.market_id) >= cap.get()
                    {
                        continue;
                    }

                    // Horizon cooldown — suppress new opens when within
                    // `no_buy_within_horizon_days` of `simulation_end`.
                    if no_buy_cutoff_unix.is_some_and(|c| sim_date_unix >= c) {
                        continue;
                    }

                    // Time-to-expiry filter: skip trades where the market's scheduled
                    // close is more than max_hours_to_expiry hours after the trade date.
                    //
                    // Fallback chain (issue #137, sub-PR 1):
                    // 1. `schedules` row with `Some(end_date_unix)` → use that timestamp
                    //    (the date the live trader would have seen).
                    // 2. `schedules` row with `None` end_date OR market absent from
                    //    schedules → fall through to `resolutions.resolved_at_unix`.
                    //    Pre-fix, the `None` case allowed every trade through; that was
                    //    an inconsistency vs. the missing-row case and the source of the
                    //    97.89% NULL coverage gap.
                    // 3. Both absent → fail-closed iff `require_known_expiry`; otherwise
                    //    allow through (preserves anti-survivorship semantics for tests
                    //    and pre-CLOB data caches).
                    if let Some(max_hours) = config.max_hours_to_expiry {
                        let max_secs = i64::from(max_hours) * 3600;
                        let end_unix = schedules
                            .get(&trade.market_id)
                            .and_then(|s| s.end_date_unix)
                            .or_else(|| {
                                resolutions
                                    .get(&trade.market_id)
                                    .map(|r| r.resolved_at_unix)
                            });
                        let suppressed = match end_unix {
                            Some(ts) => ts - sim_date_unix > max_secs,
                            None => config.require_known_expiry,
                        };
                        suppression_tracker.record(sim_date, suppressed);
                        if suppressed {
                            continue;
                        }
                    }

                    let fill_price = {
                        let raw = trade.price.0 * (Decimal::ONE + slippage_rate);
                        // Clamp to (0, 1).
                        if raw >= Decimal::ONE {
                            continue;
                        }
                        raw
                    };

                    // High-price cap (issue #142): skip BUYs whose
                    // slippage-adjusted `fill_price` is ≥ `max_signal_price`.
                    // Gating on `fill_price` (not the leader's signal price)
                    // captures the cost we'd actually pay and prevents a
                    // signal at 0.849 + 1% slippage = 0.857 from squeaking
                    // past a signal-price cap. Fires before flat-USD and
                    // Kelly so every sizing branch honors it.
                    if let Some(cap) = config.max_signal_price {
                        if fill_price >= cap {
                            high_price_tracker.record(sim_date, true);
                            continue;
                        }
                        high_price_tracker.record(sim_date, false);
                    }

                    // Flat-USD short-circuit (issue #134): backtest-only research
                    // lever that bypasses Kelly, per-trade cap, mode clamp,
                    // `risk-engine`, and the liquidity clamp. `floor(flat /
                    // fill_price).max(1)` — degenerate `fill_price > flat` opens
                    // at 1 contract (best-effort). Skip when bankroll cannot
                    // cover the full notional (all-or-nothing semantics). The
                    // Kelly-path counters (`total_signals_evaluated`,
                    // `snapshot_prior_*`, `liquidity_*`) stay zero because the
                    // corresponding code paths never execute.
                    if let Some(flat) = config.flat_usd {
                        let contracts = ((flat / fill_price).floor().to_u64().unwrap_or(1)).max(1);
                        let notional = Decimal::from(contracts) * fill_price;
                        if bankroll < notional {
                            continue;
                        }
                        bankroll -= notional;

                        let actual_bps = proposed_trade_bps(contracts, fill_price, bankroll);
                        exposure.add(leader, &trade.market_id, actual_bps);

                        open_positions.insert(
                            wallet_pos_key,
                            OpenPosition {
                                contracts,
                                avg_fill_price: fill_price,
                                bought_on: sim_date,
                                open_unix: trade.timestamp.0.unix_timestamp(),
                            },
                        );

                        if emit_period_pnl {
                            let acc = period_pnl.entry((leader, sim_date)).or_default();
                            acc.n_fills += 1;
                            acc.notional += notional;
                        }

                        let fill = TradeFill {
                            simulated_at: sim_date.midnight().assume_utc(),
                            leader_wallet: leader.to_string(),
                            market_id: trade.market_id.0.0.clone(),
                            outcome_id: trade.outcome_id.0,
                            side: "buy".to_owned(),
                            contracts,
                            signal_price: trade.price.0,
                            fill_price,
                        };
                        write_fill(fills_writer.as_mut(), &fill)?;
                        continue;
                    }

                    // Snapshot-aware prior (issue #129): leaders newly entering
                    // the candidate pool get symmetric extra pseudo-observations
                    // added to the Beta prior, weakening their thin empirical
                    // win-rate evidence. `unwrap_or(0)` is the defensive
                    // max-penalty fallback for leaders surprisingly missing from
                    // `snapshot_counts` (should not reach this path in steady
                    // state since `filtered_ledgers` is built from the current
                    // snapshot). When `!snapshots_have_data` we have no
                    // visibility signal at all — the prior is disabled.
                    total_signals_evaluated = total_signals_evaluated.saturating_add(1);
                    let n_snaps = if snapshots_have_data {
                        snapshot_counts.get(&leader).copied().unwrap_or(0)
                    } else {
                        // Sentinel = `min_snapshots` so `saturating_sub` yields 0 → extra disabled.
                        config.kelly_p_min_snapshots
                    };
                    let extra = config
                        .kelly_p_min_snapshots
                        .saturating_sub(n_snaps)
                        .saturating_mul(config.kelly_p_extra_per_missing_snapshot);
                    if extra > 0 {
                        tracing::debug!(
                            target: "snapshot_prior",
                            leader = %leader,
                            n_snaps,
                            extra,
                            "snapshot-aware prior strengthened"
                        );
                        snapshot_prior_signals = snapshot_prior_signals.saturating_add(1);
                        snapshot_prior_extra_sum =
                            snapshot_prior_extra_sum.saturating_add(u64::from(extra));
                    }
                    // Win-rate probability from the leader's ledger (Blocker 2).
                    let Some(p) = leader_win_rate_p_shrunk(
                        ledger_by_wallet.get(&leader).copied(),
                        config.kelly_p_prior_alpha,
                        config.kelly_p_prior_beta,
                        config.kelly_p_k_per_market,
                        extra,
                    ) else {
                        continue;
                    };

                    // Quality from watchlist; always present for watchlisted wallets.
                    let Some(quality) = quality_by_wallet.get(&leader).copied() else {
                        continue;
                    };

                    let signal = raw_trade_to_leader_signal(trade, quality);

                    let risk_snapshot = build_risk_snapshot(&RiskContext {
                        exposure: &exposure,
                        leader,
                        market_id: &trade.market_id,
                        intraday_bps,
                        rolling_7d_bps,
                        proposed_bps: 0, // evaluate() overwrites this
                    });

                    let contracts_count = match strategy.evaluate(
                        &signal,
                        p,
                        risk_snapshot,
                        bankroll,
                        pe_strategy_winner_follow::ExecutionMode::Paper,
                    ) {
                        Ok(intent) => intent.contracts.0,
                        Err(e) => {
                            tracing::debug!(wallet = %leader, reason = %e, "signal blocked");
                            continue;
                        }
                    };

                    // Liquidity-aware clamp — applied after evaluate() returns so
                    // the strategy contract stays pure. Partition: gate disabled
                    // → unknown market → clamp fired → below floor → data-ok
                    // (silent). See docs/_GLOSSARY.md `liquidity_take_fraction`.
                    let (liquidity_usd, market_known) = match liq_index.get(&trade.market_id) {
                        Some(&v) => (v, true),
                        None => (Decimal::ZERO, false),
                    };
                    let clamped = clamp_contracts_to_liquidity(
                        contracts_count,
                        liquidity_usd,
                        config.liquidity_take_fraction,
                        config.liquidity_min_required_usd,
                        fill_price,
                    );
                    if config.liquidity_take_fraction <= Decimal::ZERO {
                        // gate disabled — silent passthrough (data-ok bucket)
                    } else if !market_known {
                        tracing::debug!(
                            target: "liquidity_clamp",
                            market_id = %trade.market_id,
                            "no liquidity data; clamp bypassed"
                        );
                        liquidity_unknown_markets = liquidity_unknown_markets.saturating_add(1);
                    } else if clamped < contracts_count {
                        tracing::info!(
                            target: "liquidity_clamp",
                            market_id = %trade.market_id,
                            original = contracts_count,
                            clamped,
                            liquidity_usd = %liquidity_usd,
                            "liquidity clamp fired"
                        );
                        liquidity_clamps_fired = liquidity_clamps_fired.saturating_add(1);
                        liquidity_clamp_contracts_reduced = liquidity_clamp_contracts_reduced
                            .saturating_add(contracts_count - clamped);
                    } else if liquidity_usd > Decimal::ZERO
                        && liquidity_usd < config.liquidity_min_required_usd
                    {
                        tracing::warn!(
                            target: "liquidity_clamp",
                            market_id = %trade.market_id,
                            liquidity_usd = %liquidity_usd,
                            "liquidity below min_required_usd; clamp bypassed"
                        );
                        liquidity_below_floor_bypasses =
                            liquidity_below_floor_bypasses.saturating_add(1);
                    }
                    let contracts_count = clamped;

                    if contracts_count == 0 {
                        continue;
                    }

                    // Record fill.
                    let notional = Decimal::from(contracts_count) * fill_price;
                    bankroll -= notional;

                    let actual_bps = proposed_trade_bps(contracts_count, fill_price, bankroll);
                    exposure.add(leader, &trade.market_id, actual_bps);

                    open_positions.insert(
                        wallet_pos_key,
                        OpenPosition {
                            contracts: contracts_count,
                            avg_fill_price: fill_price,
                            bought_on: sim_date,
                            open_unix: trade.timestamp.0.unix_timestamp(),
                        },
                    );

                    let fill = TradeFill {
                        simulated_at: sim_date.midnight().assume_utc(),
                        leader_wallet: leader.to_string(),
                        market_id: trade.market_id.0.0.clone(),
                        outcome_id: trade.outcome_id.0,
                        side: "buy".to_owned(),
                        contracts: contracts_count,
                        signal_price: trade.price.0,
                        fill_price,
                    };
                    write_fill(fills_writer.as_mut(), &fill)?;
                }

                Side::Sell => {
                    // Close existing copy position for this leader on this (market, outcome).
                    let Some(open) = open_positions.remove(&wallet_pos_key) else {
                        continue;
                    };

                    let fill_price = {
                        let raw = trade.price.0 * (Decimal::ONE - slippage_rate);
                        if raw <= Decimal::ZERO {
                            Decimal::new(1, 4)
                        } else {
                            raw
                        }
                    };

                    // Use our full position size: the position is already removed from
                    // open_positions above, and the leader's sell quantity is their sizing,
                    // not ours. Partial-close accounting would require updating the position
                    // in-place; since we remove it, we must realize the full copy position.
                    let closed_contracts = open.contracts;
                    let revenue = Decimal::from(closed_contracts) * fill_price;
                    let cost = Decimal::from(closed_contracts) * open.avg_fill_price;
                    let pnl = revenue - cost;

                    bankroll += revenue;
                    intraday_realized_pnl += pnl;

                    let bps_removed =
                        proposed_trade_bps(closed_contracts, open.avg_fill_price, bankroll);
                    exposure.remove(leader, &trade.market_id, bps_removed);

                    pnl_accum.record(pnl);

                    if emit_period_pnl {
                        period_pnl.entry((leader, sim_date)).or_default().realized += pnl;
                    }

                    if mtm.is_some() {
                        mtm_lifetimes.push(PositionLifetime {
                            wallet: leader,
                            market: trade.market_id.clone(),
                            outcome: trade.outcome_id,
                            contracts: open.contracts,
                            avg_fill_price: open.avg_fill_price,
                            open_unix: open.open_unix,
                            close_unix: Some(trade.timestamp.0.unix_timestamp()),
                        });
                    }

                    let fill = TradeFill {
                        simulated_at: sim_date.midnight().assume_utc(),
                        leader_wallet: leader.to_string(),
                        market_id: trade.market_id.0.0.clone(),
                        outcome_id: trade.outcome_id.0,
                        side: "sell".to_owned(),
                        contracts: closed_contracts,
                        signal_price: trade.price.0,
                        fill_price,
                    };
                    write_fill(fills_writer.as_mut(), &fill)?;
                }
            }
        }
    }

    // #445 defect 3: close copied positions whose market resolved inside the configured forward-MTM
    // window but were never reached by the per-day resolution sweep — a position that resolves after
    // the last copied-wallet trade date (no later trade to advance the simulation date axis) would
    // otherwise survive to the open-ended MTM pass below and be marked at a STALE CLOB mid instead of
    // realized at its known 0/1 outcome. Bounded: iterates only the still-open copied positions and
    // closes those whose `resolved_at_unix` is in `(bought, horizon]`, stamping realized P&L on the
    // RESOLUTION day so it lands inside the Python `(as_of, horizon]` window; the closed lifetime
    // (`close_unix = resolved_at`) makes `build_mtm_rows` treat it as resolved, not open-at-horizon.
    // Only runs with a forward-MTM window (the injected bake-off path); the full-cache run is
    // unaffected. Resolutions AFTER the horizon stay open — correctly marked + lag-reported.
    if let Some((_win_start, win_end, _marks)) = mtm {
        let to_close: Vec<(WalletAddress, PosKey)> = open_positions
            .iter()
            .filter_map(|((leader, pos_key), open)| {
                let res = resolutions.get(&pos_key.0)?;
                let bought_unix = open.bought_on.midnight().assume_utc().unix_timestamp();
                if res.resolved_at_unix < bought_unix || res.resolved_at_unix > win_end {
                    return None;
                }
                Some((*leader, pos_key.clone()))
            })
            .collect();
        for (leader, pos_key) in to_close {
            let Some(open) = open_positions.remove(&(leader, pos_key.clone())) else {
                continue;
            };
            let Some(res) = resolutions.get(&pos_key.0) else {
                continue;
            };
            let realized_day = OffsetDateTime::from_unix_timestamp(res.resolved_at_unix)
                .map_err(|e| {
                    BacktestError::Internal(format!(
                        "resolved_at_unix {} out of range: {e}",
                        res.resolved_at_unix
                    ))
                })?
                .date();
            settle_position(
                leader,
                &pos_key,
                &open,
                res,
                realized_day,
                &mut bankroll,
                &mut intraday_realized_pnl,
                &mut exposure,
                &mut pnl_accum,
                &mut period_pnl,
                &mut mtm_lifetimes,
                emit_period_pnl,
                true,
                fills_writer.as_mut(),
            )?;
        }
    }

    // Positions still open at the simulation END are excluded from realized PnL —
    // their final value is unknown. Capital remains tied up in the bankroll (cost
    // was deducted at BUY). Named `open_at_sim_end` (Phase F / F3b) to disambiguate
    // from the per-window `PeriodPnlRow.open_at_horizon` (Phase E forward MTM).
    let open_at_sim_end = u64::try_from(open_positions.len()).unwrap_or(u64::MAX);
    // Do NOT modify bankroll or record PnL for open positions.

    // Forward mark-to-market rows for the configured bake-off window (issue #436
    // Phase E). Record the still-open positions (open at sim end) as open-ended
    // lifetimes, then build the per-wallet flow + coverage rows. Empty when MTM is
    // not enabled, so `pnl_by_period.ndjson` keeps its `unrealized_pnl = 0.0` shape.
    let mtm_rows: Vec<PeriodPnlRow> = if let Some((win_start, win_end, marks)) = mtm {
        for ((wallet, pos_key), open) in &open_positions {
            mtm_lifetimes.push(PositionLifetime {
                wallet: *wallet,
                market: pos_key.0.clone(),
                outcome: pos_key.1,
                contracts: open.contracts,
                avg_fill_price: open.avg_fill_price,
                open_unix: open.open_unix,
                close_unix: None,
            });
        }
        build_mtm_rows(&mtm_lifetimes, win_start, win_end, marks, resolutions)
    } else {
        Vec::new()
    };

    daily_bankroll.push(bankroll);
    daily_pnl.push(intraday_realized_pnl);

    let total_pnl_usd = daily_pnl.iter().sum::<Decimal>();
    let sharpe = sharpe_ratio(&daily_pnl);
    let max_dd = max_drawdown_pct(&daily_bankroll);

    suppression_tracker.warn_high_quarters(SUPPRESSION_WARN_THRESHOLD);
    high_price_tracker.warn_high_quarters(SUPPRESSION_WARN_THRESHOLD);

    let report = WinnerFollowReport {
        total_pnl_usd,
        sharpe_ratio: sharpe,
        max_drawdown_pct: max_dd,
        total_copies: pnl_accum.total_copies,
        win_rate_pct: pnl_accum.win_rate_pct(),
        per_operator_pnl: pnl_accum.per_operator,
        simulation_start,
        simulation_end,
        bankroll_initial,
        bankroll_final: bankroll,
        slippage_assumption_bps,
        open_at_sim_end,
        expiry_filter_suppression_pct: suppression_tracker.suppression_pct_global(),
        expiry_suppression_by_quarter: suppression_tracker.per_quarter_suppression(),
        high_price_suppression_pct: high_price_tracker.suppression_pct_global(),
        high_price_suppression_by_quarter: high_price_tracker.per_quarter_suppression(),
        total_signals_evaluated,
        snapshot_prior_signals,
        snapshot_prior_extra_sum,
        liquidity_clamps_fired,
        liquidity_clamp_contracts_reduced,
        liquidity_below_floor_bypasses,
        liquidity_unknown_markets,
        resolved_config: None,
    };

    if write_output {
        let report_path = config.output_dir.join("report.json");
        let json = serde_json::to_vec_pretty(&report)?;
        let tmp = report_path.with_extension("json.tmp");
        std::fs::write(&tmp, &json)?;
        std::fs::rename(&tmp, &report_path)?;
    }

    if emit_period_pnl {
        write_period_pnl(&config.output_dir, &period_pnl, mtm_rows)?;
    }

    info!(
        total_copies = report.total_copies,
        total_pnl_usd = %report.total_pnl_usd,
        win_rate_pct = %report.win_rate_pct,
        open_at_sim_end = report.open_at_sim_end,
        "simulation complete"
    );

    Ok(report)
}

/// Backward-compatible 9-argument entry point: run the walk-forward simulation
/// in ranker mode (no injected set). Delegates to [`run_simulation_with`] with
/// `injected = None`. Existing callers (the scenario tests and the Kelly sweep)
/// use this; only the #421 injected-set path in `main` calls
/// [`run_simulation_with`] directly.
#[allow(clippy::too_many_arguments)]
pub fn run_simulation(
    config: &BacktestConfig,
    all_trades: &[RawTrade],
    snapshots: &LeaderboardSnapshots,
    resolutions: &ResolutionIndex,
    schedules: &ScheduleIndex,
    liq_index: &LiquidityIndex,
    ranker_config: &RankerConfig,
    strategy: &WinnerFollowStrategy,
    write_output: bool,
) -> Result<WinnerFollowReport, BacktestError> {
    run_simulation_with(
        config,
        all_trades,
        snapshots,
        resolutions,
        schedules,
        liq_index,
        ranker_config,
        strategy,
        write_output,
        None,
        None,
    )
}

// ── Kelly-fraction sweep parallelism ──────────────────────────────────────────
//
// The sweep harness runs N independent simulations that differ only in the Kelly
// sizing constant. `SweepContext<'a>` bundles the read-only inputs once so each
// fraction's simulation can borrow them concurrently via rayon.
//
// Send + Sync rationale:
// - All bundled fields are immutable shared borrows (`&T` where T: Send + Sync).
// - `run_simulation` is a pure function over typed snapshots: zero global state,
//   zero RNG, all per-fraction mutable state is stack-local (`bankroll`,
//   `open_positions`, `exposure`, `pnl_accum`, accumulators, `suppression_tracker`).
// - `WalletCache` (which wraps a non-`Sync` `rusqlite::Connection`) is deliberately
//   excluded — by the time the sweep loop runs, all data has been moved out of the
//   cache into the owned vectors and indices held by `main`.
// - `fills_writer` is `None` in sweep mode (`write_output = false`), so no
//   file-handle is shared across fraction threads.

/// Read-only inputs shared by all Kelly-fraction sweep runs.
///
/// Constructed once in `main`, borrowed by `par_iter` across rayon threads.
/// The struct itself contains only shared references — `Send + Sync` are
/// derived automatically from the field types.
pub struct SweepContext<'a> {
    pub config: &'a BacktestConfig,
    pub all_trades: &'a [RawTrade],
    pub snapshots: &'a LeaderboardSnapshots,
    pub resolutions: &'a ResolutionIndex,
    pub schedules: &'a ScheduleIndex,
    pub liq_index: &'a LiquidityIndex,
    pub ranker_config: &'a RankerConfig,
}

/// Run a single Kelly-fraction iteration of the sweep against `ctx`.
///
/// Forwards the pre-sorted trade slice borrowed from `SweepContext` into
/// `run_simulation` together with a per-fraction `WinnerFollowStrategy` whose
/// `kelly_fraction_override = Some(kf)`. `write_output = false` suppresses
/// per-fraction file outputs; the caller serializes the combined
/// `KellySweepReport`.
///
/// Logging: emits a structured `tracing::info!` at start and completion of each
/// run with `kelly_fraction` and `thread` fields. Under rayon, lines from
/// different threads will interleave in stdout — query by field, not by line
/// position.
pub fn run_one_kelly_fraction(
    kf: KellyFraction,
    ctx: &SweepContext<'_>,
) -> Result<KellySweepRun, BacktestError> {
    let strategy = WinnerFollowStrategy::new(WinnerFollowConfig {
        kelly_fraction_override: Some(kf),
        ..ctx.config.strategy.clone()
    });
    let thread_id = format!("{:?}", std::thread::current().id());
    info!(
        kelly_fraction = %kf.0,
        thread = %thread_id,
        "backtest: sweep run starting"
    );
    let report = run_simulation(
        ctx.config,
        ctx.all_trades,
        ctx.snapshots,
        ctx.resolutions,
        ctx.schedules,
        ctx.liq_index,
        ctx.ranker_config,
        &strategy,
        false,
    )?;
    info!(
        kelly_fraction = %kf.0,
        thread = %thread_id,
        total_pnl_usd = %report.total_pnl_usd,
        sharpe_ratio = %report.sharpe_ratio,
        max_drawdown_pct = %report.max_drawdown_pct,
        "backtest: sweep run complete"
    );
    Ok(KellySweepRun {
        kelly_fraction: kf,
        report,
    })
}

// ── helpers ───────────────────────────────────────────────────────────────────

/// Build a `LeaderSignal` from a raw simulation trade for routing through evaluate().
///
/// All backtest signals use the `Add` action (BUY entry).
fn raw_trade_to_leader_signal(trade: &RawTrade, quality: ReconstructionQuality) -> LeaderSignal {
    LeaderSignal {
        leader: TraderId(trade.wallet),
        venue: VenueId::polymarket(),
        market_id: trade.market_id.clone(),
        outcome_id: trade.outcome_id,
        action: LeaderAction::Add,
        leader_side: trade.side,
        leader_price: trade.price,
        leader_size: Quantity(trade.contracts),
        observed_at: trade.timestamp.0,
        received_at: trade.timestamp.0,
        reconstruction_quality: quality,
        source_trade_id: trade.source_trade_id.clone(),
        action_confidence_ppm: ProbabilityPpm(700_000),
    }
}

/// Bayesian shrinkage estimate of leader win-rate probability with two additive priors.
///
/// Beta-Binomial formula:
///
/// ```text
///   N_eff       = min(total, distinct_markets × k)    (when k > 0; else N_eff = total)
///   scaled_wins = wins × N_eff / total                (Decimal — no integer truncation)
///   p_shrunk    = (scaled_wins + α + extra) / (N_eff + α + β + 2·extra)
/// ```
///
/// `(α, β, k)` are the baseline Beta(α, β) shrinkage knobs:
///   - `(α=0, β=0, k=0)` reduces to the raw empirical rate.
///   - `(α=10, β=10, k=6)` is the default: shrinks small-sample extremes toward 0.5 and
///     down-weights specialists with narrow market breadth.
///
/// `extra` is the snapshot-aware additive prior (issue #129): the caller computes
/// `extra = min_snapshots.saturating_sub(n_snapshots) × extra_per_missing` so a leader
/// that has been on fewer than `min_snapshots` leaderboards gets symmetric pseudo-
/// observations added to both numerator and denominator. As `extra` grows, the
/// effective prior point migrates from `α/(α+β)` toward 0.5 (least-informative).
///
/// Returns `None` only when `total == 0` and the combined prior carries no weight
/// (`α + β + extra == 0`).
///
/// Formula canonical reference: `docs/_GLOSSARY.md`
/// (`kelly_p_prior_alpha_default`, `kelly_p_prior_beta_default`,
/// `kelly_p_k_per_market_default`, `kelly_p_min_snapshots_default`,
/// `kelly_p_extra_per_missing_snapshot_default`).
///
/// # Precondition
/// Caller should ensure the ledger has passed the bootstrap filter (>15 closed trades,
/// >95% win rate). This function does not re-apply those thresholds.
fn leader_win_rate_p_shrunk(
    ledger: Option<&TraderLedger>,
    alpha: u32,
    beta: u32,
    k_per_market: u32,
    extra: u32,
) -> Option<Probability> {
    let ledger = ledger?;
    let wins = ledger
        .closed_trades
        .iter()
        .filter(|t| t.realized_pnl_usd > Decimal::ZERO)
        .count() as u32;
    let total = ledger.closed_trades.len() as u32;
    if total == 0 && (alpha + beta + extra) == 0 {
        return None;
    }
    let (scaled_wins, effective_n) = if k_per_market == 0 || total == 0 {
        (Decimal::from(wins), total)
    } else {
        let distinct = ledger
            .closed_trades
            .iter()
            .map(|t| &t.market_id)
            .collect::<HashSet<_>>()
            .len() as u32;
        let n_eff = total.min(distinct.saturating_mul(k_per_market));
        let sw = Decimal::from(wins) * Decimal::from(n_eff) / Decimal::from(total);
        (sw, n_eff)
    };
    // Symmetric snapshot-aware prior: numerator gains `extra`, denominator
    // gains `2 * extra`. As `extra` grows the effective prior point migrates
    // from α/(α+β) toward 0.5 — by design, the snapshot-aware prior pulls
    // toward least-informative when data is thin.
    let num = scaled_wins + Decimal::from(alpha) + Decimal::from(extra);
    let den_u = effective_n
        .saturating_add(alpha)
        .saturating_add(beta)
        .saturating_add(extra.saturating_mul(2));
    let den = Decimal::from(den_u);
    Probability::new((num / den).clamp(Decimal::ZERO, Decimal::ONE)).ok()
}

/// Settle one resolved copied position at its 0/1 outcome price: pay revenue into the bankroll,
/// update exposure / the P&L accumulator / the intraday tally, stamp the realized P&L on
/// `realized_day`, optionally record the closed lifetime for forward MTM, and log the resolution
/// fill. Shared by the per-day resolution sweep and the #445 end-of-run sweep — the latter closes
/// positions whose market resolves inside the MTM window with NO later copied-wallet trade to
/// advance the date axis, which the per-day sweep would otherwise miss (leaving the position to be
/// wrongly marked at a stale CLOB mid instead of realized at its known outcome).
#[allow(clippy::too_many_arguments)]
fn settle_position(
    leader: WalletAddress,
    pos_key: &PosKey,
    open: &OpenPosition,
    res: &MarketResolution,
    realized_day: Date,
    bankroll: &mut Decimal,
    intraday_realized_pnl: &mut Decimal,
    exposure: &mut ExposureTracker,
    pnl_accum: &mut PnlAccumulator,
    period_pnl: &mut HashMap<(WalletAddress, Date), PeriodAccum>,
    mtm_lifetimes: &mut Vec<PositionLifetime>,
    emit_period_pnl: bool,
    record_lifetime: bool,
    fills_writer: Option<&mut std::fs::File>,
) -> Result<(), BacktestError> {
    let close_price = if res.winning_outcome_id == pos_key.1 {
        Decimal::ONE
    } else {
        Decimal::ZERO
    };

    let revenue = Decimal::from(open.contracts) * close_price;
    let cost = Decimal::from(open.contracts) * open.avg_fill_price;
    let pnl = revenue - cost;
    *bankroll += revenue;
    *intraday_realized_pnl += pnl;

    let bps_removed = proposed_trade_bps(open.contracts, open.avg_fill_price, *bankroll);
    exposure.remove(leader, &pos_key.0, bps_removed);

    pnl_accum.record(pnl);

    if emit_period_pnl {
        period_pnl
            .entry((leader, realized_day))
            .or_default()
            .realized += pnl;
    }

    if record_lifetime {
        mtm_lifetimes.push(PositionLifetime {
            wallet: leader,
            market: pos_key.0.clone(),
            outcome: pos_key.1,
            contracts: open.contracts,
            avg_fill_price: open.avg_fill_price,
            open_unix: open.open_unix,
            close_unix: Some(res.resolved_at_unix),
        });
    }

    let fill = TradeFill {
        simulated_at: realized_day.midnight().assume_utc(),
        leader_wallet: leader.to_string(),
        market_id: pos_key.0.0.0.clone(),
        outcome_id: pos_key.1.0,
        side: "resolution".to_owned(),
        contracts: open.contracts,
        signal_price: close_price,
        fill_price: close_price,
    };
    write_fill(fills_writer, &fill)?;
    Ok(())
}

fn proposed_trade_bps(contracts: u64, price: Decimal, bankroll: Decimal) -> i32 {
    if bankroll <= Decimal::ZERO {
        return 0;
    }
    let notional = Decimal::from(contracts) * price;
    let bps = (notional / bankroll) * Decimal::from(10_000u32);
    bps.floor().to_i32().unwrap_or(0)
}

fn decimal_to_bps(pnl: Decimal, bankroll: Decimal) -> i32 {
    if bankroll <= Decimal::ZERO {
        return 0;
    }
    let bps = (pnl / bankroll) * Decimal::from(10_000u32);
    bps.floor().to_i32().unwrap_or(0)
}

struct RiskContext<'a> {
    exposure: &'a ExposureTracker,
    leader: WalletAddress,
    market_id: &'a MarketId,
    intraday_bps: i32,
    rolling_7d_bps: i32,
    proposed_bps: i32,
}

fn build_risk_snapshot(ctx: &RiskContext<'_>) -> RiskSnapshot {
    RiskSnapshot {
        leader_exposure_bps: BasisPoints(ctx.exposure.leader_bps(ctx.leader)),
        market_exposure_bps: BasisPoints(ctx.exposure.market_bps(ctx.market_id)),
        family_exposure_bps: BasisPoints(0),
        total_copy_exposure_bps: BasisPoints(ctx.exposure.total),
        intraday_pnl_bps: BasisPoints(ctx.intraday_bps),
        rolling_7d_pnl_bps: BasisPoints(ctx.rolling_7d_bps),
        onchain_source_status: SourceStatus::Healthy,
        copy_latency_p95_ms: 0,
        trading_mode: TradingMode::LiveTiny,
        proposed_trade_bps: BasisPoints(ctx.proposed_bps),
        per_trade_cap_bps: 0, // evaluate() overwrites with resolved cap from WinnerFollowConfig
    }
}

fn maybe_open_trades_ndjson(
    output_dir: &Path,
    write_output: bool,
) -> Result<Option<std::fs::File>, BacktestError> {
    if !write_output {
        return Ok(None);
    }
    std::fs::create_dir_all(output_dir)?;
    let path = output_dir.join("trades.ndjson");
    Ok(Some(std::fs::File::create(path)?))
}

fn write_fill(writer: Option<&mut std::fs::File>, fill: &TradeFill) -> Result<(), BacktestError> {
    let Some(w) = writer else {
        return Ok(());
    };
    let line = serde_json::to_string(fill)?;
    writeln!(w, "{line}")?;
    Ok(())
}

/// Per-(wallet, UTC day) copy-P&L accumulator for the #421 injected-set handoff.
/// Realized P&L lands on the day a position closes; `n_fills`/`notional` on the
/// day a copy opens (so a buy and its eventual settlement can fall in different
/// rows — the harness sums across periods).
#[derive(Debug, Default, Clone)]
struct PeriodAccum {
    realized: Decimal,
    n_fills: u64,
    notional: Decimal,
}

/// One emitted row of `pnl_by_period.ndjson` (the #421 `pe-backtest` → Python
/// policy handoff). `period_end` is the unix second at the END of the UTC
/// calendar day the activity fell in (next midnight) for realized day rows.
///
/// Two row kinds (issue #436 Phase E):
/// - **Realized day row** (`is_horizon_mtm = false`): per (wallet, UTC day),
///   carrying `realized_pnl` / `n_fills` / `notional`. `unrealized_pnl = 0`.
/// - **Horizon MTM row** (`is_horizon_mtm = true`): per wallet, stamped at
///   `period_end = mtm_window_end_unix`, carrying the forward mark-to-market
///   *flow* in `unrealized_pnl` (`Σ(mark_H − cost) − Σ(mark_as_of − cost)`), the
///   `(open_at_horizon, marked_at_horizon)` coverage counts, and the F3a
///   resolution-lag diagnostics (`positions_in_window`, `resolution_lags_secs`).
///   `realized_pnl = 0`. The Python harness sums `realized_pnl + unrealized_pnl`
///   over the window for the objective, and filters `~is_horizon_mtm` for the
///   realized-only demoter/`live_pnl` accumulation. The F3a fields are advisory
///   reporting only — they never enter the objective or the verdict.
///
/// When no MTM window is configured, only realized day rows are emitted and
/// `unrealized_pnl` stays the documented `0.0` sentinel. The money fields
/// serialize as JSON numbers so the Python harness reads them as floats.
#[derive(Debug, Serialize)]
struct PeriodPnlRow {
    wallet: String,
    period_end: i64,
    #[serde(serialize_with = "serialize_decimal_as_f64")]
    realized_pnl: Decimal,
    #[serde(serialize_with = "serialize_decimal_as_f64")]
    unrealized_pnl: Decimal,
    n_fills: u64,
    #[serde(serialize_with = "serialize_decimal_as_f64")]
    notional: Decimal,
    /// `true` only for the forward MTM horizon row (issue #436 Phase E); the
    /// realized-only demoter/`live_pnl` path filters these out.
    is_horizon_mtm: bool,
    /// Count of this wallet's positions still open at the horizon (MTM rows only).
    open_at_horizon: u64,
    /// Of `open_at_horizon`, how many had a CLOB mark at-or-before the horizon
    /// (the rest are uncovered → contribute `0`). Coverage = marked / open.
    marked_at_horizon: u64,
    /// Positions this wallet held that were *active in the window* — open at the
    /// horizon (`open_at_horizon`) plus those it closed within `(as_of, horizon]`
    /// (resolved or sold). Denominator for the open-at-horizon fraction
    /// `open_at_horizon / positions_in_window` (issue #436 Phase F / F3a). `0` on
    /// realized day rows. Carried only on a SURVIVING MTM row — one with open-at-
    /// horizon exposure OR a non-zero boundary flow; a wallet whose entire window
    /// footprint is positions opened AND closed strictly inside the window (no mark at
    /// either boundary) emits no row, so those count toward neither side.
    positions_in_window: u64,
    /// Per open-at-horizon position, the resolution lag `resolved_at − as_of` in
    /// seconds (issue #436 Phase F / F3a) — how long after the window opened the
    /// still-unresolved position eventually settled. `-1` marks a *censored*
    /// position whose market never resolved in the cache. ADVISORY ONLY: built from
    /// post-horizon resolution times, it never feeds the bake-off verdict (which is
    /// realized + MTM-flow). Empty on realized day rows.
    resolution_lags_secs: Vec<i64>,
}

/// Build the forward mark-to-market rows for one bake-off window (issue #436
/// Phase E). Per wallet, `unrealized_pnl` is the window *flow*
/// `Σ_{open at horizon}(mark_H − cost) − Σ_{open at as_of}(mark_as_of − cost)` —
/// a flow (not a stock `mark − cost`), so summing realized + unrealized across
/// the trajectory's adjacent windows telescopes to each position's true lifecycle
/// P&L with no double-count, and the realized day rows stay untouched. A position
/// open at a boundary with no CLOB mark there contributes `0` (coverage-bounded,
/// E3). One row per wallet that carried a boundary position (all-zero rows are
/// dropped), stamped at `period_end = win_end` so the Python window slice
/// `(as_of, horizon]` includes it.
///
/// **Why the telescoping holds even under partial coverage.** Consecutive bake-off
/// windows are *adjacent* — `as_of` of window k+1 equals `win_end` of window k (B3
/// enforces `step_days ≥ horizon_days`; the operator default is `==`), so the shared
/// boundary is marked at the SAME timestamp in both windows. `mark_at_or_before` is a
/// pure function of that timestamp, so window k's `win_end` leg and window k+1's
/// `as_of` leg are byte-identical and cancel — covered or not. A position uncovered
/// at `as_of` (no CLOB sample at-or-before it) was therefore equally uncovered at the
/// prior window's `win_end` (same instant), which credited nothing; crediting
/// `(mark_H − cost)` in the first covered window is the position's single, correct
/// first valuation — NOT a `mark − cost` stock double-count. (With `step_days >
/// horizon_days` the windows are gapped and the inter-window interval is simply not
/// evaluated — a deliberate skip, never a double-count.)
fn build_mtm_rows(
    lifetimes: &[PositionLifetime],
    win_start: i64,
    win_end: i64,
    marks: &ClobMarkIndex,
    resolutions: &ResolutionIndex,
) -> Vec<PeriodPnlRow> {
    #[derive(Default)]
    struct WalletMtm {
        unreal_start: Decimal,
        unreal_end: Decimal,
        open_at_horizon: u64,
        marked_at_horizon: u64,
        positions_in_window: u64,
        resolution_lags_secs: Vec<i64>,
    }
    let mut by_wallet: HashMap<WalletAddress, WalletMtm> = HashMap::new();
    for lt in lifetimes {
        let contracts = Decimal::from(lt.contracts);
        if lt.open_at(win_start)
            && let Some(mark) = marks.mark_at_or_before(&lt.market, lt.outcome, win_start)
        {
            by_wallet.entry(lt.wallet).or_default().unreal_start +=
                (mark - lt.avg_fill_price) * contracts;
        }
        if lt.open_at(win_end) {
            let entry = by_wallet.entry(lt.wallet).or_default();
            entry.open_at_horizon += 1;
            entry.positions_in_window += 1;
            // Resolution lag (advisory diagnostic, issue #436 Phase F / F3a): how long
            // after `as_of` (= `win_start`) this still-open position eventually settled.
            // `-1` = the market never resolved in the cache (censored). The resolution
            // time is post-horizon, so this is REPORTING ONLY and never feeds the verdict.
            let lag = resolutions
                .get(&lt.market)
                .map_or(-1, |r| r.resolved_at_unix - win_start);
            entry.resolution_lags_secs.push(lag);
            if let Some(mark) = marks.mark_at_or_before(&lt.market, lt.outcome, win_end) {
                entry.unreal_end += (mark - lt.avg_fill_price) * contracts;
                entry.marked_at_horizon += 1;
            }
        } else if lt.close_unix.is_some_and(|c| c > win_start && c <= win_end) {
            // Closed within the window (resolved or sold): no flow or lag, but it
            // belongs in the open-fraction denominator. Carried only if the wallet's
            // row survives the drop filter below (open-at-horizon exposure OR a
            // non-zero boundary flow — e.g. a position covered at `as_of`).
            by_wallet.entry(lt.wallet).or_default().positions_in_window += 1;
        }
    }
    by_wallet
        .into_iter()
        .filter_map(|(wallet, m)| {
            let flow = m.unreal_end - m.unreal_start;
            // Drop pure-noise rows (no flow and no horizon exposure to report).
            if flow.is_zero() && m.open_at_horizon == 0 {
                return None;
            }
            Some(PeriodPnlRow {
                wallet: wallet.to_string(),
                period_end: win_end,
                realized_pnl: Decimal::ZERO,
                unrealized_pnl: flow,
                n_fills: 0,
                notional: Decimal::ZERO,
                is_horizon_mtm: true,
                open_at_horizon: m.open_at_horizon,
                marked_at_horizon: m.marked_at_horizon,
                positions_in_window: m.positions_in_window,
                resolution_lags_secs: m.resolution_lags_secs,
            })
        })
        .collect()
}

/// Serialize a `Decimal` as a JSON float at the output boundary (the Python
/// harness schema is `…:float`). The conversion never feeds back into P&L math,
/// so it does not violate the no-`f64`-for-money rule; an out-of-range value is
/// a hard error rather than a silent truncation.
fn serialize_decimal_as_f64<S>(d: &Decimal, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let v = d
        .to_f64()
        .ok_or_else(|| S::Error::custom(format!("decimal {d} not representable as f64")))?;
    serializer.serialize_f64(v)
}

/// Bounded trade load for the #421 injected-set path: returns ONLY the trades
/// belonging to `wallets` (deduped), never scanning the full cache. This is what
/// makes running with `max_trade_count = 0` safe — the bake-off harness sets that
/// to disable the global `cache.trade_count()` backstop, so the load itself must
/// be self-bounding.
///
/// The returned trades are in wallet-then-cache order, NOT sorted by timestamp —
/// the caller MUST sort by `t.timestamp.0` before passing them to
/// [`run_simulation_with`] (its documented precondition). `main` does exactly this.
pub fn load_injected_trades(cache: &WalletCache, wallets: &[WalletAddress]) -> Vec<RawTrade> {
    let mut seen: HashSet<WalletAddress> = HashSet::new();
    let mut trades: Vec<RawTrade> = Vec::new();
    for wallet in wallets {
        if !seen.insert(*wallet) {
            continue; // de-dup: a repeated wallet must not double-load its trades
        }
        trades.extend(cache.trades_for(&wallet.to_string()));
    }
    trades
}

/// Write `pnl_by_period.ndjson` to `output_dir` — one NDJSON line per
/// (wallet, period), sorted by (wallet bytes, day) for byte-stable output. See
/// [`PeriodPnlRow`] for the schema and the `unrealized_pnl` sentinel.
fn write_period_pnl(
    output_dir: &Path,
    period_pnl: &HashMap<(WalletAddress, Date), PeriodAccum>,
    mtm_rows: Vec<PeriodPnlRow>,
) -> Result<(), BacktestError> {
    std::fs::create_dir_all(output_dir)?;
    let path = output_dir.join("pnl_by_period.ndjson");
    let mut file = std::fs::File::create(path)?;

    // Assemble realized day rows (is_horizon_mtm = false) + the forward MTM
    // horizon rows (issue #436 Phase E) into one set.
    let mut rows: Vec<PeriodPnlRow> = period_pnl
        .iter()
        .map(|((wallet, day), acc)| PeriodPnlRow {
            wallet: wallet.to_string(),
            period_end: day.midnight().assume_utc().unix_timestamp() + 86_400,
            realized_pnl: acc.realized,
            unrealized_pnl: Decimal::ZERO,
            n_fills: acc.n_fills,
            notional: acc.notional,
            is_horizon_mtm: false,
            open_at_horizon: 0,
            marked_at_horizon: 0,
            positions_in_window: 0,
            resolution_lags_secs: Vec::new(),
        })
        .collect();
    rows.extend(mtm_rows);

    // Deterministic order: wallet hex, then period_end, then row kind (realized
    // day rows before the horizon MTM row when they share a period_end).
    rows.sort_by(|a, b| {
        a.wallet
            .cmp(&b.wallet)
            .then(a.period_end.cmp(&b.period_end))
            .then(a.is_horizon_mtm.cmp(&b.is_horizon_mtm))
    });

    for row in &rows {
        let line = serde_json::to_string(row)?;
        writeln!(file, "{line}")?;
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    /// Compile-time regression guard for issue #156: `SweepContext::all_trades`
    /// must remain a borrow, not an owned `Vec<RawTrade>`. The function below
    /// only compiles if the field is `&[T]` or `&Vec<T>` (which deref-coerces
    /// to `&[T]`); a regression to owned `Vec<RawTrade>` would fail to compile
    /// because the inferred return type would require an unavailable copy or
    /// move out of a shared reference. The function is intentionally never
    /// called — its purpose is to be type-checked, not executed.
    #[allow(dead_code)]
    fn _assert_sweep_context_all_trades_is_borrowed<'a>(ctx: &SweepContext<'a>) -> &'a [RawTrade] {
        ctx.all_trades
    }

    fn market(s: &str) -> MarketId {
        use pe_core_types::VenueMarketId;
        MarketId(VenueMarketId(s.to_owned()))
    }

    fn wallet(b: u8) -> WalletAddress {
        WalletAddress::from_hex(&format!("0x{:040x}", b)).unwrap()
    }

    // ── injected-set bounded load (#421 PR3) ───────────────────────────────────

    fn raw_buy(w: WalletAddress, mkt: &str, id: &str, ts: i64) -> RawTrade {
        use pe_core_types::{ContractQty, Price, SourceTradeId};
        RawTrade {
            wallet: w,
            market_id: market(mkt),
            outcome_id: OutcomeId(0),
            side: Side::Buy,
            price: Price(rust_decimal_macros::dec!(0.50)),
            contracts: ContractQty(10),
            timestamp: SourceTimestamp(OffsetDateTime::from_unix_timestamp(ts).unwrap()),
            source_trade_id: SourceTradeId(id.to_owned()),
        }
    }

    // The injected-set path runs with `max_trade_count = 0` (guard disabled), so
    // the loader MUST be self-bounding: it may pull ONLY the injected wallets'
    // trades, never the full cache. (Issue #421 PR3 bounded-load AC.)
    #[test]
    fn load_injected_trades_loads_only_requested_wallets() {
        use tempfile::TempDir;
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
        let (a, b, c) = (wallet(0xa1), wallet(0xb2), wallet(0xc3));
        cache
            .insert_new(
                &a.to_string(),
                vec![
                    raw_buy(a, "mkt-a", "a-1", 1_700_000_000),
                    raw_buy(a, "mkt-a2", "a-2", 1_700_000_100),
                ],
            )
            .unwrap();
        cache
            .insert_new(
                &b.to_string(),
                vec![raw_buy(b, "mkt-b", "b-1", 1_700_000_050)],
            )
            .unwrap();
        cache
            .insert_new(
                &c.to_string(),
                vec![
                    raw_buy(c, "mkt-c", "c-1", 1_700_000_200),
                    raw_buy(c, "mkt-c2", "c-2", 1_700_000_300),
                ],
            )
            .unwrap();

        // Inject {A, B}; A repeated to exercise the de-dup guard. C must be
        // excluded — proving the load is bounded to the injected wallets.
        let loaded = load_injected_trades(&cache, &[a, b, a]);

        assert_eq!(
            loaded.len(),
            3,
            "A(2) + B(1); C excluded; A not double-loaded"
        );
        assert!(
            loaded.iter().all(|t| t.wallet == a || t.wallet == b),
            "only injected wallets' trades may be loaded"
        );
        assert!(
            !loaded.iter().any(|t| t.wallet == c),
            "non-injected wallet C must not be loaded"
        );
    }

    // ── ExposureTracker: leader and market caps ───────────────────────────────

    #[test]
    fn leader_bps_and_market_bps_track_none_operator_wallets() {
        let mut tracker = ExposureTracker::default();
        let m1 = market("mkt-f");
        let m2 = market("mkt-g");
        tracker.add(wallet(1), &m1, 25);
        tracker.add(wallet(1), &m2, 25);
        tracker.add(wallet(2), &m1, 30);
        assert_eq!(tracker.leader_bps(wallet(1)), 50);
        assert_eq!(tracker.leader_bps(wallet(2)), 30);
        assert_eq!(tracker.market_bps(&m1), 55);
        assert_eq!(tracker.market_bps(&m2), 25);
        assert_eq!(tracker.total, 80);
    }

    // ── ExposureTracker: market_position_count (issue #138) ────────────────────

    #[test]
    fn market_position_count_starts_at_zero() {
        let tracker = ExposureTracker::default();
        let m = market("mkt-h");
        assert_eq!(tracker.market_position_count(&m), 0);
    }

    #[test]
    fn add_increments_count_independent_of_bps() {
        // The position counter must increment once per `add` regardless of bps.
        // This is the invariant that prevents `remove → add(-bps)` delegation.
        let mut tracker = ExposureTracker::default();
        let m = market("mkt-i");
        tracker.add(wallet(1), &m, 250);
        tracker.add(wallet(2), &m, 1);
        tracker.add(wallet(3), &m, 5_000);
        assert_eq!(tracker.market_position_count(&m), 3);
        // bps still accumulates linearly.
        assert_eq!(tracker.market_bps(&m), 5_251);
    }

    #[test]
    fn add_then_remove_returns_count_to_zero() {
        // Critical symmetry guard: after every `add` has a matching `remove`,
        // both the count and the bps must return to 0. A future refactor that
        // drifts `add` and `remove` apart will trip this test.
        let mut tracker = ExposureTracker::default();
        let m = market("mkt-j");
        tracker.add(wallet(1), &m, 100);
        tracker.add(wallet(2), &m, 200);
        tracker.remove(wallet(1), &m, 100);
        tracker.remove(wallet(2), &m, 200);
        assert_eq!(tracker.market_position_count(&m), 0);
        assert_eq!(tracker.market_bps(&m), 0);
        assert_eq!(tracker.total, 0);
    }

    #[test]
    fn remove_on_empty_count_saturates_at_zero() {
        // Defensive guard for double-close — a panic here would convert a
        // recoverable caller bug into a fatal simulation error.
        let mut tracker = ExposureTracker::default();
        let m = market("mkt-k");
        tracker.remove(wallet(1), &m, 10);
        assert_eq!(tracker.market_position_count(&m), 0);
    }

    #[test]
    fn remove_does_not_increment_count() {
        // Locks in the split-method invariant: `remove` must NEVER increment
        // the position counter. A regression where `remove` delegates back to
        // `add(-bps)` would trip this test (count would tick to 1 on remove).
        let mut tracker = ExposureTracker::default();
        let m = market("mkt-l");
        tracker.add(wallet(1), &m, 50);
        assert_eq!(tracker.market_position_count(&m), 1);
        tracker.remove(wallet(1), &m, 50);
        assert_eq!(tracker.market_position_count(&m), 0);
        // A second remove must not increment.
        tracker.remove(wallet(2), &m, 25);
        assert_eq!(tracker.market_position_count(&m), 0);
    }

    // ── leader_win_rate_p_shrunk ───────────────────────────────────────────────

    use pe_core_types::{ContractQty, OutcomeId, Price};
    use pe_trader_index::ledger::ClosedTrade;
    use rust_decimal_macros::dec;

    fn closed_trade(pnl_usd: Decimal) -> ClosedTrade {
        ClosedTrade {
            market_id: market("mkt-0"),
            outcome_id: OutcomeId(0),
            side: Side::Buy,
            entry_price: Price(dec!(0.50)),
            exit_price: Price(dec!(1.00)),
            contracts: ContractQty(1),
            hold_duration_seconds: 86_400,
            realized_pnl_usd: pnl_usd,
            opened_at_unix: 0,
            closed_at_unix: 86_400,
            source_trade_ids: vec![],
        }
    }

    fn make_ledger(wins: usize, losses: usize) -> TraderLedger {
        let mut closed = Vec::with_capacity(wins + losses);
        for _ in 0..wins {
            closed.push(closed_trade(dec!(0.50)));
        }
        for _ in 0..losses {
            closed.push(closed_trade(dec!(-0.50)));
        }
        TraderLedger {
            wallet: wallet(0),
            reconstruction_quality: ReconstructionQuality::new(100).unwrap(),
            closed_trades: closed,
            open_positions: vec![],
            audit_window_days: 90,
        }
    }

    // (0,0) prior → raw rate, identical to old leader_win_rate_p behaviour.
    #[test]
    fn shrinkage_with_zero_priors_matches_raw() {
        let ledger = make_ledger(11, 1);
        let p = leader_win_rate_p_shrunk(Some(&ledger), 0, 0, 0, 0).unwrap();
        // 11/12 = 0.91666...
        let expected = Decimal::from(11u32) / Decimal::from(12u32);
        assert_eq!(p.0, expected.clamp(Decimal::ZERO, Decimal::ONE));
    }

    // No trades + zero prior → None (no data at all).
    #[test]
    fn no_trades_zero_prior_returns_none() {
        let ledger = make_ledger(0, 0);
        assert!(leader_win_rate_p_shrunk(Some(&ledger), 0, 0, 0, 0).is_none());
    }

    // No trades + non-zero prior → prior mean (0.5 for symmetric Beta(10,10)).
    #[test]
    fn shrinkage_with_no_trades_returns_prior_mean() {
        let ledger = make_ledger(0, 0);
        let p = leader_win_rate_p_shrunk(Some(&ledger), 10, 10, 0, 0).unwrap();
        // (0+10)/(0+20) = 0.5
        assert_eq!(p.0, dec!(0.5));
    }

    // Small sample: prior dominates.  11/12 with (10,10) → (21/32) = 0.65625.
    #[test]
    fn shrinkage_dominates_at_low_n() {
        let ledger = make_ledger(11, 1);
        let p = leader_win_rate_p_shrunk(Some(&ledger), 10, 10, 0, 0).unwrap();
        let expected = Decimal::from(21u32) / Decimal::from(32u32); // 0.65625
        assert_eq!(p.0, expected);
    }

    // Large sample: prior is negligible.  1100/1200 with (10,10) → within 0.01 of raw.
    // (1110/1220 - 1100/1200) ≈ 0.0068, well under 1% — prior strength of 20 is
    // negligible against 1200 observations.
    #[test]
    fn shrinkage_negligible_at_high_n() {
        let ledger = make_ledger(1100, 100);
        let p = leader_win_rate_p_shrunk(Some(&ledger), 10, 10, 0, 0).unwrap();
        let raw = Decimal::from(1100u32) / Decimal::from(1200u32);
        let diff = (p.0 - raw).abs();
        assert!(
            diff < dec!(0.01),
            "shrunk p = {}, raw = {}, diff = {} >= 0.01",
            p.0,
            raw,
            diff
        );
    }

    // None ledger → None regardless of prior.
    #[test]
    fn no_ledger_returns_none() {
        assert!(leader_win_rate_p_shrunk(None, 10, 10, 0, 0).is_none());
    }

    // Asymmetric prior skews estimate toward 0 when β is large.
    #[test]
    fn asymmetric_prior_skews_estimate() {
        let ledger = make_ledger(5, 5); // raw p = 0.5
        let p_low = leader_win_rate_p_shrunk(Some(&ledger), 1, 99, 0, 0).unwrap();
        // (5+1)/(10+100) = 6/110 ≈ 0.0545 — prior pulls strongly toward 0
        assert!(p_low.0 < dec!(0.10));
    }

    // All wins with symmetric prior → result is above raw/(prior pulls down).
    #[test]
    fn all_wins_prior_pulls_toward_half() {
        let ledger = make_ledger(12, 0); // raw p = 1.0
        let p = leader_win_rate_p_shrunk(Some(&ledger), 10, 10, 0, 0).unwrap();
        // (12+10)/(12+20) = 22/32 = 0.6875 — well below raw 1.0
        assert_eq!(p.0, Decimal::from(22u32) / Decimal::from(32u32));
    }

    // ── k_per_market / N_eff tests ────────────────────────────────────────────

    /// Build a ledger where `wins` winning and `losses` losing trades are distributed
    /// round-robin across `markets` distinct market IDs.
    fn make_ledger_multi_market(wins: usize, losses: usize, markets: usize) -> TraderLedger {
        let mut closed = Vec::with_capacity(wins + losses);
        for i in 0..wins {
            let mkt = format!("mkt-{}", i % markets.max(1));
            closed.push(ClosedTrade {
                market_id: market(&mkt),
                outcome_id: OutcomeId(0),
                side: Side::Buy,
                entry_price: Price(dec!(0.50)),
                exit_price: Price(dec!(1.00)),
                contracts: ContractQty(1),
                hold_duration_seconds: 86_400,
                realized_pnl_usd: dec!(0.50),
                opened_at_unix: 0,
                closed_at_unix: 86_400,
                source_trade_ids: vec![],
            });
        }
        for i in 0..losses {
            let mkt = format!("mkt-{}", (wins + i) % markets.max(1));
            closed.push(ClosedTrade {
                market_id: market(&mkt),
                outcome_id: OutcomeId(0),
                side: Side::Buy,
                entry_price: Price(dec!(0.50)),
                exit_price: Price(dec!(0.00)),
                contracts: ContractQty(1),
                hold_duration_seconds: 86_400,
                realized_pnl_usd: dec!(-0.50),
                opened_at_unix: 0,
                closed_at_unix: 86_400,
                source_trade_ids: vec![],
            });
        }
        TraderLedger {
            wallet: wallet(0),
            reconstruction_quality: ReconstructionQuality::new(100).unwrap(),
            closed_trades: closed,
            open_positions: vec![],
            audit_window_days: 90,
        }
    }

    // 60 trades on 1 market, k=6 → N_eff = min(60, 1×6) = 6.
    // scaled_wins = 57 × 6/60 = 5.7; shrunk_p = (5.7+10)/(6+20) ≈ 0.604.
    #[test]
    fn n_eff_caps_at_distinct_markets_times_k() {
        let ledger = make_ledger_multi_market(57, 3, 1); // 57 wins, 3 losses, 1 market
        let p = leader_win_rate_p_shrunk(Some(&ledger), 10, 10, 6, 0).unwrap();
        let expected = (dec!(57) * dec!(6) / dec!(60) + dec!(10)) / (dec!(6) + dec!(10) + dec!(10));
        assert_eq!(p.0, expected.clamp(Decimal::ZERO, Decimal::ONE));
    }

    // 60 trades on 60 markets, k=6 → N_eff = min(60, 60×6) = 60 (not capped).
    // scaled_wins = 57 × 60/60 = 57; shrunk_p = (57+10)/(60+20) ≈ 0.838.
    #[test]
    fn n_eff_uses_total_when_markets_are_diverse() {
        let ledger = make_ledger_multi_market(57, 3, 60); // 1 trade per market
        let p = leader_win_rate_p_shrunk(Some(&ledger), 10, 10, 6, 0).unwrap();
        let expected = (dec!(57) + dec!(10)) / (dec!(60) + dec!(10) + dec!(10));
        assert_eq!(p.0, expected.clamp(Decimal::ZERO, Decimal::ONE));
    }

    // k=0 bypasses N_eff entirely — same result as pre-#107 formula.
    #[test]
    fn k_zero_bypasses_n_eff() {
        let ledger = make_ledger_multi_market(57, 3, 1);
        let p_k0 = leader_win_rate_p_shrunk(Some(&ledger), 10, 10, 0, 0).unwrap();
        let p_bypass = leader_win_rate_p_shrunk(Some(&ledger), 10, 10, 0, 0).unwrap();
        // Both should give (57+10)/(60+20) = 67/80
        let expected = (dec!(57) + dec!(10)) / (dec!(60) + dec!(10) + dec!(10));
        assert_eq!(p_k0.0, expected.clamp(Decimal::ZERO, Decimal::ONE));
        assert_eq!(p_bypass.0, p_k0.0);
    }

    // scaled_wins uses Decimal division: 57 × 6/60 = 5.7, not integer 5.
    // Verify p differs from the integer-truncated formula.
    #[test]
    fn scaled_wins_uses_decimal_not_integer_arithmetic() {
        let ledger = make_ledger_multi_market(57, 3, 1);
        let p_decimal = leader_win_rate_p_shrunk(Some(&ledger), 10, 10, 6, 0).unwrap();
        // integer truncation would give 5; Decimal gives 5.7
        let p_integer_trunc =
            Probability::new((dec!(5) + dec!(10)) / (dec!(6) + dec!(10) + dec!(10))).unwrap();
        let p_decimal_calc =
            Probability::new((dec!(5.7) + dec!(10)) / (dec!(6) + dec!(10) + dec!(10))).unwrap();
        assert_ne!(
            p_decimal.0, p_integer_trunc.0,
            "must use Decimal division, not integer truncation"
        );
        assert_eq!(p_decimal.0, p_decimal_calc.0);
    }

    // ── snapshot-aware prior (issue #129) ──────────────────────────────────

    // `extra = 0` is the no-op case — must produce the identical result as
    // the pre-#129 formula. Locks in the backwards-compat guarantee.
    #[test]
    fn snapshot_aware_prior_disabled_when_extra_zero() {
        let ledger = make_ledger_multi_market(11, 1, 5);
        // 11/12 wins, α=β=10, k=6, extra=0.
        let p_no_extra = leader_win_rate_p_shrunk(Some(&ledger), 10, 10, 6, 0).unwrap();
        // N_eff = min(12, 5×6) = 12. scaled_wins = 11×12/12 = 11. p = 21/32 ≈ 0.656.
        let expected = (dec!(11) + dec!(10)) / (dec!(12) + dec!(10) + dec!(10));
        assert_eq!(p_no_extra.0, expected.clamp(Decimal::ZERO, Decimal::ONE));
    }

    // Worked example from issue #129: extra=20 pulls 21/32 down to 41/72 ≈ 0.569.
    #[test]
    fn snapshot_aware_prior_pulls_toward_half_for_new_entrant() {
        let ledger = make_ledger_multi_market(11, 1, 5);
        let p = leader_win_rate_p_shrunk(Some(&ledger), 10, 10, 6, 20).unwrap();
        // num = 11 + 10 + 20 = 41; den = 12 + 10 + 10 + 40 = 72.
        let expected = dec!(41) / dec!(72);
        assert_eq!(p.0, expected.clamp(Decimal::ZERO, Decimal::ONE));
    }

    // With asymmetric prior (α=20, β=5 → prior point 0.8), extra=20 pulls the
    // effective prior point toward 0.5. Documented behavior in issue #129.
    #[test]
    fn snapshot_aware_prior_shifts_effective_prior_point_with_asymmetric_alpha_beta() {
        // Empty ledger so the result is the pure prior point.
        let empty_ledger = make_ledger_multi_market(0, 0, 0);
        // Without extra: 20/(20+5) = 0.8.
        let p_no_extra = leader_win_rate_p_shrunk(Some(&empty_ledger), 20, 5, 0, 0).unwrap();
        assert_eq!(p_no_extra.0, dec!(20) / dec!(25));
        // With extra=20: (0+20+20)/(0+20+5+40) = 40/65 ≈ 0.615 — shifted toward 0.5.
        let p_with_extra = leader_win_rate_p_shrunk(Some(&empty_ledger), 20, 5, 0, 20).unwrap();
        assert_eq!(p_with_extra.0, dec!(40) / dec!(65));
        // Must be strictly less than the no-extra value (moving toward 0.5 from 0.8).
        assert!(p_with_extra.0 < p_no_extra.0);
        // And strictly greater than 0.5 (asymmetric prior not fully collapsed).
        assert!(p_with_extra.0 > dec!(0.5));
    }

    // No-ledger + non-zero extra → still returns the pure-prior probability.
    // Guards against a regression where extra was ignored when ledger is None.
    #[test]
    fn snapshot_aware_prior_with_extra_only_returns_pure_prior() {
        let ledger = make_ledger_multi_market(0, 0, 0);
        let p = leader_win_rate_p_shrunk(Some(&ledger), 0, 0, 0, 10).unwrap();
        // num = 0+0+10 = 10; den = 0+0+0+20 = 20. p = 0.5.
        assert_eq!(p.0, dec!(0.5));
    }
}
