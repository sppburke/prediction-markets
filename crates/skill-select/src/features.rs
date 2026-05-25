//! Per-wallet feature extraction over a wallet's train-window ledger (issue #212).
//!
//! This slice computes the **simple deterministic** feature batch — counts,
//! realized PnL, ROI, win rate, and mean hold — directly from the reconstructed
//! `ClosedTrade` vector. The distribution-moment features (Sharpe / skewness /
//! kurtosis / LCB), calibration, and buy-and-hold benchmark land in a later
//! slice; the sign-randomization skill statistic and Deflated Sharpe are filled
//! by the skill-test / selection phases. Because this produces a
//! [`DeterministicFeatures`] value and writes nothing, no field is ever
//! persisted with a placeholder.
//!
//! **Why not reuse `pe_trader_index::compute_stats`?** Its `win_rate_bps` /
//! `lcb_5pct_bps` are double-scaled — it passes `ratio × 10_000` into
//! `BasisPoints::from_decimal`, which multiplies by 10_000 again (`score.rs`
//! win-rate / `lcb_5pct`), yielding values ×10_000 too large. The ranker is
//! internally self-consistent so its *ranking* is unaffected, but the absolute
//! "bps" are wrong, and DSR/BHq selection here needs correct magnitudes. So this
//! crate computes its bps directly with a single ×10_000. (Tracked as an
//! out-of-scope trader-index fix.)
//!
//! All money/ratio arithmetic uses `rust_decimal` (no `f64`); basis-point
//! conversions saturate rather than wrap.

use std::collections::HashMap;
use std::collections::HashSet;

use pe_bootstrap::cache::ResolutionIndex;
use pe_core_types::{MarketId, VenueMarketId};
use pe_trader_index::{ClosedTrade, TraderLedger};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::{Decimal, MathematicalOps};

/// The simple deterministic per-wallet feature batch computed at a given cutoff.
///
/// A subset of the persisted `wallet_features` columns; grows additively as
/// later slices add the distribution-moment / calibration / buy-and-hold
/// features. Assembled into the full `WalletFeatures` row (with the skill-test
/// and DSR outputs) by the extraction phase.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeterministicFeatures {
    /// `0x`-prefixed lowercase wallet address.
    pub wallet_hex: String,
    /// Train/forward split this batch was computed at (unix seconds, UTC).
    pub cutoff_unix: i64,
    /// FIFO reconstruction quality, 0..=100.
    pub reconstruction_quality: u8,
    /// Closed trades in the train window (`closed_at ≤ cutoff`).
    pub closed_trades: u32,
    /// Distinct traded markets in the train window.
    pub distinct_markets: u32,
    /// Distinct events (neg-risk bundles, via the market→event map) in the window.
    pub distinct_events: u32,
    /// Sum of realized PnL over the train window (USD).
    pub total_pnl_usd: Decimal,
    /// Return on cost (`total_pnl / total_cost`), basis points; `0` when cost is zero.
    pub roi_bps: i64,
    /// Empirical win rate (`wins / closed`), basis points (0–10 000).
    pub win_rate_bps: i32,
    /// Mean hold duration over closed trades, seconds.
    pub avg_hold_secs: i64,
    /// Distinct UTC calendar days with a closed trade (the daily-return series length).
    pub trading_days: u32,
    /// Mean of the daily return-on-cost series, basis points.
    pub mean_daily_return_bps: i64,
    /// Population standard deviation of the daily-return series, basis points.
    pub std_daily_return_bps: i64,
    /// Per-period Sharpe (`mean / std`) of the daily-return series × 10_000;
    /// `0` when `std` is zero (DSR applies the √n scaling later).
    pub sharpe_bps: i64,
    /// Fisher skewness of the daily-return series × 10_000; `0` when `std` is zero.
    pub skewness_bps: i64,
    /// Excess kurtosis (kurtosis − 3) of the daily-return series × 10_000; `0` when `std` is zero.
    pub excess_kurtosis_bps: i64,
    /// Lower-confidence bound `mean − 1.645·(std/√n)` of the daily-return series,
    /// basis points (5th-pct one-sided). Equals the mean when `n < 2`.
    pub lcb_5pct_bps: i32,
    // ─── Per-bet quality (docs/24- §3.1 group A) ───────────────────────────────
    //
    // Computed over closed trades whose market has a resolution row
    // (`ResolutionIndex`); unresolved-market trades are excluded from per-bet
    // and per-event statistics (mirroring forward.rs). `closed_trades` is NOT
    // reduced — it still counts all in-window closed trades.
    /// Mean of per-bet entry→resolution edge `(o − c)` over resolved closed
    /// trades, basis points. `o ∈ {0, 1}` is the bought-outcome indicator;
    /// `c` is the entry price. `0` when no resolved closed trades exist.
    pub ev_mean_bps: i64,
    /// `t = √n · mean(o − c) / sd(o − c)` × 10⁴ over resolved closed trades;
    /// `0` when `n < 2` or population std is zero. Naive (assumes IID); the
    /// event-block bootstrap variant is a later PR.
    pub ev_tstat_bps: i64,
    /// Beta-binomial shrunk edge `p̂ − c̄` × 10⁴ where `p̂ = (x + α) / (n + α + β)`
    /// (`x` = wins among resolved buys, `n` = resolved-buy count, `α`/`β` from
    /// config) and `c̄` is the mean entry price over resolved buys. `0` when
    /// `n` is zero.
    pub bb_shrunk_edge_bps: i64,
    /// Exact binary Kelly log-growth at the shrunk-edge prior:
    /// `g* = p̂·ln(1 + b·f*) + (1 − p̂)·ln(1 − f*)` with `b = (1 − c̄) / c̄` and
    /// `f* = (p̂ − c̄) / (1 − c̄)`, then × 10⁴. `0` when `f* ≤ 0`, when `c̄`
    /// degenerates (`≤ 0` or `≥ 1`), or when no resolved buys exist.
    pub kelly_log_growth_bps: i64,
    /// Brier score `(1 / N) · Σ (c − o)²` × 10⁴ over resolved closed trades —
    /// lower = better calibration. `0` when no resolved closed trades exist.
    pub brier_score_bps: i64,
    /// Murphy 1973 resolution component `(1 / N) · Σ nₖ (ōₖ − ō)²` × 10⁴,
    /// bucketing entries by **entry-price decile** (width 0.10, matching
    /// `forward_price_bucket_width_bps`). Higher = better discrimination.
    /// `0` when no resolved closed trades exist.
    pub brier_resolution_bps: i64,
    // ─── Concentration (docs/24- §3.1 group F) ─────────────────────────────────
    //
    // Computed over per-event positive realized PnL roll-ups (resolved or not —
    // PnL exists for any closed trade). Events without positive PnL contribute
    // nothing to the share denominator.
    /// HHI `Σ s_i²` × 10⁴ over per-event profit shares
    /// `s_i = positive_event_pnl_i / Σ positive_event_pnl`. `0` when no event
    /// has positive PnL.
    pub concentration_hhi_bps: i32,
    /// Effective number of profit events `1 / HHI` × 10⁴ — i.e.
    /// `10⁸ / hhi_bps`. Saturates at `i32::MAX`; `0` when HHI is zero.
    pub concentration_n_eff_bps: i32,
    /// Rank-weighted concentration `Σ r · s_i(r)` × 10⁴ with shares sorted
    /// descending (rank 1 = largest share). For a perfectly equal distribution
    /// over `N` events this equals `(N + 1) / 2 × 10⁴`. `0` when no event has
    /// positive PnL.
    pub concentration_rpc_bps: i64,
    // ─── Activity (docs/24- §3.1 group C) ──────────────────────────────────────
    /// `distinct_first_entries / trading_days` × 10⁴. Distinct first-entries
    /// equals `distinct_markets` (one first-entry per market). `0` when
    /// `trading_days` is zero. Saturates at `i32::MAX`.
    pub first_entries_per_active_day_bps: i32,
    // ─── Capital velocity (docs/24- §3.1 group D) ─────────────────────────────
    /// Median of `(market_resolved_at_unix − first_entry_ts)` over distinct
    /// markets whose first-entry is in-window AND have a resolution row;
    /// **resolutions before the first entry (data anomaly)** are excluded.
    /// Seconds. `0` when no qualifying market exists (sentinel only — never
    /// downstream-gated).
    pub median_first_entry_to_resolution_secs: i64,
}

/// Compute the simple deterministic feature batch for one wallet's train ledger.
///
/// Returns `None` when the wallet has no closed trades with `closed_at ≤
/// cutoff_unix`, fewer than `min_closed_trades` of them (ineligible), or fewer
/// than `min_distinct_events` distinct events traded (SSRN 6617059 §C gate).
///
/// `resolutions` supplies the bought-outcome / resolution-time pair per market
/// for the per-bet quality (group A), Brier, and capital-velocity features;
/// trades whose market lacks a resolution row are excluded from those
/// statistics but still counted in the sample-size fields (`closed_trades`,
/// `distinct_markets`, `distinct_events`).
///
/// # Precondition
/// `ledger` should already be reconstructed from trades `≤ cutoff_unix` (the
/// extraction phase partitions raw trades at the cutoff before building the
/// ledger). This function additionally filters closed trades by `closed_at ≤
/// cutoff_unix` defensively, so a stray post-cutoff close cannot leak into the
/// train features.
#[allow(clippy::too_many_arguments)] // canonical extraction call; one site.
pub fn extract_features(
    ledger: &TraderLedger,
    cutoff_unix: i64,
    event_map: &HashMap<String, String>,
    resolutions: &ResolutionIndex,
    min_closed_trades: u32,
    min_distinct_events: u32,
    bb_alpha: u32,
    bb_beta: u32,
) -> Option<DeterministicFeatures> {
    let windowed: Vec<&_> = ledger
        .closed_trades
        .iter()
        .filter(|t| t.closed_at_unix <= cutoff_unix)
        .collect();

    if windowed.is_empty() {
        return None;
    }
    let closed_trades = u32::try_from(windowed.len()).unwrap_or(u32::MAX);
    if closed_trades < min_closed_trades {
        return None;
    }

    // Money: exact Decimal sums; ROI in basis points, saturating on conversion.
    let total_pnl_usd: Decimal = windowed.iter().map(|t| t.realized_pnl_usd).sum();
    let total_cost: Decimal = windowed
        .iter()
        .map(|t| t.entry_price.0 * Decimal::from(t.contracts.0))
        .sum();
    let roi_bps = if total_cost.is_zero() {
        0
    } else {
        decimal_to_bps_i64(total_pnl_usd / total_cost)
    };

    // Win rate over the window: fraction of closed trades with positive PnL.
    let wins = windowed
        .iter()
        .filter(|t| t.realized_pnl_usd > Decimal::ZERO)
        .count();
    let win_ratio = Decimal::from(u64::try_from(wins).unwrap_or(0)) / Decimal::from(closed_trades);
    let win_rate_bps = i32::try_from(decimal_to_bps_i64(win_ratio)).unwrap_or(10_000);

    let distinct_markets = windowed
        .iter()
        .map(|t| &t.market_id.0.0)
        .collect::<HashSet<&String>>()
        .len();

    // Distinct events: map each market to its event_id; orphans self-map to the
    // market id (so an unmapped market still counts as its own event).
    let distinct_events = windowed
        .iter()
        .map(|t| {
            let market = &t.market_id.0.0;
            event_map.get(market).unwrap_or(market)
        })
        .collect::<HashSet<&String>>()
        .len();
    let distinct_events_u32 = u32::try_from(distinct_events).unwrap_or(u32::MAX);
    if distinct_events_u32 < min_distinct_events {
        // SSRN 6617059 §C gate: below the paper's distinct-events threshold the
        // skill test is under-powered. Drop the wallet before downstream stats.
        return None;
    }

    // Mean hold: sum in u128 to avoid overflow, then saturate into i64.
    let total_hold: u128 = windowed
        .iter()
        .map(|t| u128::from(t.hold_duration_seconds))
        .sum();
    let count = u128::try_from(windowed.len()).unwrap_or(1).max(1);
    let avg_hold_secs = i64::try_from(total_hold / count).unwrap_or(i64::MAX);

    // Daily return-on-cost series → distribution moments (DSR / LCB inputs).
    let daily = daily_return_series(&windowed);
    let m = compute_moments(&daily);

    // Per-bet (o, c) pairs over resolved-market trades only.
    let per_bet = per_bet_outcomes(&windowed, resolutions);
    let per_bet_quality = compute_per_bet_quality(&per_bet, bb_alpha, bb_beta);

    // Per-event positive-PnL roll-up over all in-window closed trades
    // (resolved or not — realized PnL exists either way; group F is a
    // diversification stat, not a calibration stat).
    let per_event_pos_pnl = per_event_positive_pnl(&windowed, event_map);
    let concentration = compute_concentration(&per_event_pos_pnl);

    let first_entries_per_active_day_bps = compute_first_entries_per_active_day(
        u32::try_from(distinct_markets).unwrap_or(u32::MAX),
        u32::try_from(daily.len()).unwrap_or(u32::MAX),
    );

    let median_first_entry_to_resolution_secs =
        compute_median_first_entry_to_resolution(&windowed, resolutions);

    Some(DeterministicFeatures {
        wallet_hex: ledger.wallet.to_string(),
        cutoff_unix,
        reconstruction_quality: ledger.reconstruction_quality.get(),
        closed_trades,
        distinct_markets: u32::try_from(distinct_markets).unwrap_or(u32::MAX),
        distinct_events: distinct_events_u32,
        total_pnl_usd,
        roi_bps,
        win_rate_bps,
        avg_hold_secs,
        trading_days: u32::try_from(daily.len()).unwrap_or(u32::MAX),
        mean_daily_return_bps: decimal_to_bps_i64(m.mean),
        std_daily_return_bps: decimal_to_bps_i64(m.std),
        sharpe_bps: decimal_to_bps_i64(m.sharpe),
        skewness_bps: decimal_to_bps_i64(m.skewness),
        excess_kurtosis_bps: decimal_to_bps_i64(m.excess_kurtosis),
        lcb_5pct_bps: i32::try_from(decimal_to_bps_i64(m.lcb_5pct)).unwrap_or_else(|_| {
            if m.lcb_5pct.is_sign_negative() {
                i32::MIN
            } else {
                i32::MAX
            }
        }),
        ev_mean_bps: per_bet_quality.ev_mean_bps,
        ev_tstat_bps: per_bet_quality.ev_tstat_bps,
        bb_shrunk_edge_bps: per_bet_quality.bb_shrunk_edge_bps,
        kelly_log_growth_bps: per_bet_quality.kelly_log_growth_bps,
        brier_score_bps: per_bet_quality.brier_score_bps,
        brier_resolution_bps: per_bet_quality.brier_resolution_bps,
        concentration_hhi_bps: concentration.hhi_bps,
        concentration_n_eff_bps: concentration.n_eff_bps,
        concentration_rpc_bps: concentration.rpc_bps,
        first_entries_per_active_day_bps,
        median_first_entry_to_resolution_secs,
    })
}

/// Build the per-UTC-day return-on-cost series: each trade's return is
/// `realized_pnl / (entry_price × contracts)`; per-day returns sum the trades
/// that closed that day (`day = floor(closed_at / 86_400)`). Mirrors the daily
/// bucketing convention used elsewhere in the workspace.
fn daily_return_series(windowed: &[&ClosedTrade]) -> Vec<Decimal> {
    let mut by_day: HashMap<i64, Decimal> = HashMap::new();
    for t in windowed {
        let cost = t.entry_price.0 * Decimal::from(t.contracts.0);
        let r = if cost.is_zero() {
            Decimal::ZERO
        } else {
            t.realized_pnl_usd / cost
        };
        let day = t.closed_at_unix.div_euclid(86_400);
        *by_day.entry(day).or_insert(Decimal::ZERO) += r;
    }
    by_day.into_values().collect()
}

/// Distribution moments of a return series (all `Decimal`, no `f64`).
struct Moments {
    mean: Decimal,
    std: Decimal,
    sharpe: Decimal,
    skewness: Decimal,
    excess_kurtosis: Decimal,
    lcb_5pct: Decimal,
}

/// Population moments of `series`. `std`/`sharpe`/`skewness`/`excess_kurtosis`
/// are `0` when the series has fewer than 2 points or zero dispersion (no
/// dispersion ⇒ those shape stats are undefined; `0` is the safe sentinel).
/// `lcb_5pct = mean − 1.645·(std/√n)` (one-sided 5th pct); equals `mean` when
/// `n < 2`. Higher powers use repeated multiplication (no `powi`); `sqrt` via
/// the `maths` feature.
fn compute_moments(series: &[Decimal]) -> Moments {
    let n = series.len();
    if n == 0 {
        return Moments {
            mean: Decimal::ZERO,
            std: Decimal::ZERO,
            sharpe: Decimal::ZERO,
            skewness: Decimal::ZERO,
            excess_kurtosis: Decimal::ZERO,
            lcb_5pct: Decimal::ZERO,
        };
    }
    let n_dec = Decimal::from(u64::try_from(n).unwrap_or(u64::MAX));
    let mean = series.iter().copied().sum::<Decimal>() / n_dec;

    if n < 2 {
        // Single point: no dispersion; LCB is the mean (stderr = 0).
        return Moments {
            mean,
            std: Decimal::ZERO,
            sharpe: Decimal::ZERO,
            skewness: Decimal::ZERO,
            excess_kurtosis: Decimal::ZERO,
            lcb_5pct: mean,
        };
    }

    let mut m2 = Decimal::ZERO;
    let mut m3 = Decimal::ZERO;
    let mut m4 = Decimal::ZERO;
    for &x in series {
        let d = x - mean;
        let d2 = d * d;
        m2 += d2;
        m3 += d2 * d;
        m4 += d2 * d2;
    }
    m2 /= n_dec; // variance (population)
    m3 /= n_dec;
    m4 /= n_dec;

    let std = m2.sqrt().unwrap_or(Decimal::ZERO);
    let (sharpe, skewness, excess_kurtosis) = if std.is_zero() {
        (Decimal::ZERO, Decimal::ZERO, Decimal::ZERO)
    } else {
        let std3 = std * std * std;
        let std4 = std3 * std;
        // std3/std4 can underflow to Decimal::ZERO when std is extremely small
        // (Decimal has 28-digit precision; std^3 for std≈1e-10 ≈ 1e-30, below the floor).
        // Treat underflow the same as zero dispersion: shape stats are undefined.
        if std3.is_zero() || std4.is_zero() {
            (mean / std, Decimal::ZERO, Decimal::ZERO)
        } else {
            (mean / std, m3 / std3, m4 / std4 - Decimal::from(3u32))
        }
    };

    // stderr = std / sqrt(n); LCB = mean − 1.645 · stderr.
    let stderr = std / n_dec.sqrt().unwrap_or(Decimal::ONE);
    let lcb_5pct = mean - dec_const_1_645() * stderr;

    Moments {
        mean,
        std,
        sharpe,
        skewness,
        excess_kurtosis,
        lcb_5pct,
    }
}

/// The one-sided 5th-percentile normal z-score, `1.645`, as an exact `Decimal`.
fn dec_const_1_645() -> Decimal {
    Decimal::new(1645, 3)
}

// ─── Per-bet quality (docs/24- §3.1 group A) ──────────────────────────────────

/// One closed-trade observation with a known resolution outcome: entry price
/// `c` ∈ (0, 1] and bought-outcome indicator `o` ∈ {0, 1}. Per the data
/// contract, unresolved-market trades never produce an entry here.
struct PerBet {
    /// Entry price per $1 contract.
    c: Decimal,
    /// `1` if the bought outcome won the resolution, else `0`.
    o: Decimal,
}

/// Build the `(o, c)` per-bet table over the window's closed trades, joining
/// against `resolutions`. Unresolved markets and markets that resolved before
/// the trade opened (data anomaly) are skipped — matches forward.rs.
fn per_bet_outcomes(windowed: &[&ClosedTrade], resolutions: &ResolutionIndex) -> Vec<PerBet> {
    let mut out = Vec::with_capacity(windowed.len());
    for t in windowed {
        let key = MarketId(VenueMarketId(t.market_id.0.0.clone()));
        let Some(res) = resolutions.get(&key) else {
            continue;
        };
        // Data-anomaly guard: a resolution stamped before the trade opened is
        // unphysical. Drop rather than score (mirrors forward.rs).
        if res.resolved_at_unix < t.opened_at_unix {
            continue;
        }
        let o = if res.winning_outcome_id == t.outcome_id {
            Decimal::ONE
        } else {
            Decimal::ZERO
        };
        out.push(PerBet {
            c: t.entry_price.0,
            o,
        });
    }
    out
}

/// Output of the per-bet quality block (group A).
struct PerBetQuality {
    ev_mean_bps: i64,
    ev_tstat_bps: i64,
    bb_shrunk_edge_bps: i64,
    kelly_log_growth_bps: i64,
    brier_score_bps: i64,
    brier_resolution_bps: i64,
}

/// Compute the group-A per-bet quality stats from the `(o, c)` table.
/// Returns all-zeros when `per_bet` is empty (sentinel — no observable
/// per-bet quality without resolved trades).
fn compute_per_bet_quality(per_bet: &[PerBet], bb_alpha: u32, bb_beta: u32) -> PerBetQuality {
    let n = per_bet.len();
    if n == 0 {
        return PerBetQuality {
            ev_mean_bps: 0,
            ev_tstat_bps: 0,
            bb_shrunk_edge_bps: 0,
            kelly_log_growth_bps: 0,
            brier_score_bps: 0,
            brier_resolution_bps: 0,
        };
    }
    let n_dec = Decimal::from(u64::try_from(n).unwrap_or(u64::MAX));

    // EV = mean(o − c) over resolved buys.
    let edges: Vec<Decimal> = per_bet.iter().map(|p| p.o - p.c).collect();
    let ev_mean = edges.iter().copied().sum::<Decimal>() / n_dec;

    // EV t-stat: √n · mean / std (population). 0 when n<2 or std=0.
    let ev_tstat = if n < 2 {
        Decimal::ZERO
    } else {
        let mut var = Decimal::ZERO;
        for &e in &edges {
            let d = e - ev_mean;
            var += d * d;
        }
        var /= n_dec;
        let std = var.sqrt().unwrap_or(Decimal::ZERO);
        if std.is_zero() {
            Decimal::ZERO
        } else {
            let root_n = n_dec.sqrt().unwrap_or(Decimal::ONE);
            root_n * ev_mean / std
        }
    };

    // Beta-binomial shrunk edge: p̂ = (x + α) / (n + α + β) minus c̄.
    let wins: u32 = per_bet
        .iter()
        .filter(|p| p.o > Decimal::ZERO)
        .count()
        .try_into()
        .unwrap_or(u32::MAX);
    let alpha = Decimal::from(bb_alpha);
    let beta = Decimal::from(bb_beta);
    let p_hat = (Decimal::from(wins) + alpha) / (n_dec + alpha + beta);
    let c_bar: Decimal = per_bet.iter().map(|p| p.c).sum::<Decimal>() / n_dec;
    let bb_shrunk_edge = p_hat - c_bar;

    // Exact binary Kelly log-growth: g* = p̂·ln(1 + b·f*) + (1−p̂)·ln(1 − f*)
    // with b = (1 − c̄)/c̄ and f* = (p̂ − c̄)/(1 − c̄). 0 when f* ≤ 0 or c̄
    // degenerates outside (0, 1) — the binary-bet formula assumes both
    // outcomes have positive probability under c̄.
    let kelly_g = if c_bar <= Decimal::ZERO || c_bar >= Decimal::ONE {
        Decimal::ZERO
    } else {
        let one_minus_c = Decimal::ONE - c_bar;
        let f_star = (p_hat - c_bar) / one_minus_c;
        if f_star <= Decimal::ZERO {
            Decimal::ZERO
        } else {
            let b = one_minus_c / c_bar;
            let win_term = (Decimal::ONE + b * f_star).ln();
            let loss_term = (Decimal::ONE - f_star).ln();
            p_hat * win_term + (Decimal::ONE - p_hat) * loss_term
        }
    };

    // Brier score = mean((c − o)²); lower = better.
    let brier_sum: Decimal = per_bet
        .iter()
        .map(|p| {
            let d = p.c - p.o;
            d * d
        })
        .sum();
    let brier_score = brier_sum / n_dec;

    // Brier resolution component (Murphy 1973): bucket entries by entry-price
    // decile (width 0.10), then `(1/N) · Σ nₖ(ōₖ − ō)²`. Higher = better
    // discrimination across price bands.
    let o_bar: Decimal = per_bet.iter().map(|p| p.o).sum::<Decimal>() / n_dec;
    // 10 deciles + 1 catch-all for c == 1.0; collect (count, win_sum) per bucket.
    let mut buckets: HashMap<u8, (u64, Decimal)> = HashMap::new();
    for p in per_bet {
        let bucket = price_decile_bucket(p.c);
        let entry = buckets.entry(bucket).or_insert((0, Decimal::ZERO));
        entry.0 += 1;
        entry.1 += p.o;
    }
    let mut res_sum = Decimal::ZERO;
    for (nk, win_sum) in buckets.values() {
        if *nk == 0 {
            continue;
        }
        let nk_dec = Decimal::from(*nk);
        let o_bar_k = win_sum / nk_dec;
        let d = o_bar_k - o_bar;
        res_sum += nk_dec * d * d;
    }
    let brier_resolution = res_sum / n_dec;

    PerBetQuality {
        ev_mean_bps: decimal_to_bps_i64(ev_mean),
        ev_tstat_bps: decimal_to_bps_i64(ev_tstat),
        bb_shrunk_edge_bps: decimal_to_bps_i64(bb_shrunk_edge),
        kelly_log_growth_bps: decimal_to_bps_i64(kelly_g),
        brier_score_bps: decimal_to_bps_i64(brier_score),
        brier_resolution_bps: decimal_to_bps_i64(brier_resolution),
    }
}

/// Entry-price decile bucket: `floor(c / 0.10)` clamped to `0..=10`. `c = 1.0`
/// lands in bucket `10`; bucket `10` therefore exists for at-cap prices but
/// gets no per-bet quality math anyway (degenerate `c̄` short-circuits Kelly).
fn price_decile_bucket(c: Decimal) -> u8 {
    let width = Decimal::new(1, 1); // 0.1
    if c < Decimal::ZERO {
        return 0;
    }
    let bucket = (c / width).floor().to_u32().unwrap_or(u32::from(u8::MAX));
    u8::try_from(bucket).unwrap_or(u8::MAX).min(10)
}

// ─── Concentration (docs/24- §3.1 group F) ────────────────────────────────────

/// Sum positive realized PnL by event-id for the window. Negative-PnL events
/// contribute nothing to the share denominator (HHI is a profit-concentration
/// stat, not a turnover stat).
fn per_event_positive_pnl<'a>(
    windowed: &'a [&'a ClosedTrade],
    event_map: &'a HashMap<String, String>,
) -> HashMap<&'a String, Decimal> {
    let mut by_event: HashMap<&String, Decimal> = HashMap::new();
    for t in windowed {
        let market = &t.market_id.0.0;
        let event = event_map.get(market).unwrap_or(market);
        *by_event.entry(event).or_insert(Decimal::ZERO) += t.realized_pnl_usd;
    }
    // Drop non-positive events: only positive events contribute a share.
    by_event.retain(|_, pnl| *pnl > Decimal::ZERO);
    by_event
}

/// Output of the concentration block (group F).
struct ConcentrationStats {
    hhi_bps: i32,
    n_eff_bps: i32,
    rpc_bps: i64,
}

/// Compute HHI, N_eff, and RPC from the per-event positive PnL map. All-zeros
/// when no event has positive PnL.
fn compute_concentration(per_event_pos_pnl: &HashMap<&String, Decimal>) -> ConcentrationStats {
    if per_event_pos_pnl.is_empty() {
        return ConcentrationStats {
            hhi_bps: 0,
            n_eff_bps: 0,
            rpc_bps: 0,
        };
    }
    let total: Decimal = per_event_pos_pnl.values().copied().sum();
    if total <= Decimal::ZERO {
        return ConcentrationStats {
            hhi_bps: 0,
            n_eff_bps: 0,
            rpc_bps: 0,
        };
    }

    // Shares + descending sort for the rank-weighted concentration.
    let mut shares: Vec<Decimal> = per_event_pos_pnl.values().map(|v| *v / total).collect();
    // Decimal is Ord (total-ordered for finite values); descending.
    shares.sort_by(|a, b| b.cmp(a));

    // HHI = Σ s²; in bps = Σ s² × 10⁴.
    let hhi: Decimal = shares.iter().map(|s| *s * *s).sum();
    let hhi_bps_i64 = decimal_to_bps_i64(hhi);
    let hhi_bps = i32::try_from(hhi_bps_i64).unwrap_or(i32::MAX);

    // N_eff = 1 / HHI; in bps = (1 / HHI) × 10⁴ = 10⁸ / hhi_bps. Saturate i32.
    let n_eff_bps = if hhi_bps_i64 <= 0 {
        0
    } else {
        i32::try_from(100_000_000i64 / hhi_bps_i64).unwrap_or(i32::MAX)
    };

    // RPC = Σ r · s_r (r = 1..=N), shares sorted descending. Equals
    // (N + 1) / 2 for the perfectly equal distribution.
    let mut rpc = Decimal::ZERO;
    for (idx, s) in shares.iter().enumerate() {
        let r = Decimal::from(u64::try_from(idx).unwrap_or(u64::MAX) + 1);
        rpc += r * *s;
    }
    let rpc_bps = decimal_to_bps_i64(rpc);

    ConcentrationStats {
        hhi_bps,
        n_eff_bps,
        rpc_bps,
    }
}

// ─── Activity (docs/24- §3.1 group C) ─────────────────────────────────────────

/// `distinct_first_entries / trading_days` × 10⁴, saturating to `i32::MAX`.
/// Distinct first-entries equals `distinct_markets` (one first-entry per market
/// by construction). `0` when `trading_days` is zero.
fn compute_first_entries_per_active_day(distinct_markets: u32, trading_days: u32) -> i32 {
    if trading_days == 0 {
        return 0;
    }
    let ratio = Decimal::from(distinct_markets) / Decimal::from(trading_days);
    let bps = decimal_to_bps_i64(ratio);
    i32::try_from(bps).unwrap_or(i32::MAX)
}

// ─── Capital velocity (docs/24- §3.1 group D) ─────────────────────────────────

/// Median of `(market_resolved_at_unix − first_entry_ts_unix)` over distinct
/// markets whose first-entry is in the window AND have a resolution row.
/// Resolutions stamped before the first entry are excluded (data anomaly).
/// Returns `0` when no qualifying market exists (sentinel — never a downstream
/// gate).
fn compute_median_first_entry_to_resolution(
    windowed: &[&ClosedTrade],
    resolutions: &ResolutionIndex,
) -> i64 {
    // First (earliest open_at) per market over the window.
    let mut first_open: HashMap<&String, i64> = HashMap::new();
    for t in windowed {
        let market = &t.market_id.0.0;
        let entry = first_open.entry(market).or_insert(t.opened_at_unix);
        if t.opened_at_unix < *entry {
            *entry = t.opened_at_unix;
        }
    }
    let mut deltas: Vec<i64> = Vec::with_capacity(first_open.len());
    for (market, first_ts) in first_open {
        let key = MarketId(VenueMarketId(market.clone()));
        let Some(res) = resolutions.get(&key) else {
            continue;
        };
        if res.resolved_at_unix < first_ts {
            continue;
        }
        deltas.push(res.resolved_at_unix - first_ts);
    }
    if deltas.is_empty() {
        return 0;
    }
    deltas.sort_unstable();
    let mid = deltas.len() / 2;
    if deltas.len() % 2 == 1 {
        deltas[mid]
    } else {
        // Even-length: average of the two middles. Use i128 to avoid overflow
        // before halving, then saturate back to i64.
        let lo = i128::from(deltas[mid - 1]);
        let hi = i128::from(deltas[mid]);
        i64::try_from((lo + hi) / 2).unwrap_or(i64::MAX)
    }
}

/// Convert a ratio to basis points (`× 10_000`, rounded half-even), saturating
/// to the `i64` range rather than wrapping or panicking. No `f64` — `to_i64`
/// reads the `Decimal` representation directly.
fn decimal_to_bps_i64(ratio: Decimal) -> i64 {
    let bps = (ratio * Decimal::from(10_000i64))
        .round_dp_with_strategy(0, rust_decimal::RoundingStrategy::MidpointNearestEven);
    bps.to_i64().unwrap_or(if bps.is_sign_negative() {
        i64::MIN
    } else {
        i64::MAX
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use pe_bootstrap::cache::MarketResolution;
    use pe_core_types::{
        ContractQty, OutcomeId, Price, ReconstructionQuality, Side, WalletAddress,
    };
    use pe_trader_index::ClosedTrade;
    use rust_decimal_macros::dec;

    // Default-everything test args (empty resolutions, no event-count gate,
    // Laplace prior). Keeps the v1-era tests focused on the v1 fields they
    // covered without dragging the new-field arity through every call site.
    fn no_res() -> ResolutionIndex {
        ResolutionIndex::new()
    }

    fn wallet() -> WalletAddress {
        WalletAddress::from_hex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap()
    }

    /// A closed trade with the given market, entry, size, pnl, hold, close time.
    fn closed(
        market: &str,
        entry: Decimal,
        contracts: u64,
        pnl: Decimal,
        hold_secs: u64,
        closed_at: i64,
    ) -> ClosedTrade {
        ClosedTrade {
            market_id: MarketId(VenueMarketId(market.to_owned())),
            outcome_id: OutcomeId(0),
            side: Side::Buy,
            entry_price: Price::new(entry).unwrap(),
            exit_price: Price::new(dec!(1.0)).unwrap(),
            contracts: ContractQty(contracts),
            hold_duration_seconds: hold_secs,
            realized_pnl_usd: pnl,
            opened_at_unix: closed_at - i64::try_from(hold_secs).unwrap_or(0),
            closed_at_unix: closed_at,
            source_trade_ids: vec![],
        }
    }

    fn res(winning_outcome: u16, resolved_at: i64) -> MarketResolution {
        MarketResolution {
            winning_outcome_id: OutcomeId(winning_outcome),
            resolved_at_unix: resolved_at,
        }
    }

    fn market(id: &str) -> MarketId {
        MarketId(VenueMarketId(id.to_owned()))
    }

    fn ledger(closed_trades: Vec<ClosedTrade>) -> TraderLedger {
        TraderLedger {
            wallet: wallet(),
            operator_id: None,
            reconstruction_quality: ReconstructionQuality::new(100).unwrap(),
            closed_trades,
            open_positions: vec![],
            audit_window_days: 0,
        }
    }

    #[test]
    fn computes_deterministic_batch_by_hand() {
        // Markets m1,m2 share event "evtA"; m3 unmapped → self-maps.
        let mut events = HashMap::new();
        events.insert("0xm1".to_owned(), "evtA".to_owned());
        events.insert("0xm2".to_owned(), "evtA".to_owned());
        let l = ledger(vec![
            closed("0xm1", dec!(0.50), 100, dec!(10.0), 3_600, 1_000),
            closed("0xm2", dec!(0.50), 100, dec!(-5.0), 7_200, 2_000),
            closed("0xm3", dec!(0.20), 50, dec!(2.0), 1_800, 3_000),
        ]);

        let f = extract_features(&l, 10_000, &events, &no_res(), 1, 0, 1, 1).unwrap();

        assert_eq!(f.total_pnl_usd, dec!(7.0)); // 10 - 5 + 2
        assert_eq!(f.roi_bps, 636); // 7 / 110 = 0.063636… → 636 bps
        assert_eq!(f.closed_trades, 3);
        assert_eq!(f.distinct_markets, 3);
        assert_eq!(f.distinct_events, 2); // {evtA, 0xm3}
        assert_eq!(f.avg_hold_secs, 4_200); // (3600+7200+1800)/3
        assert_eq!(f.reconstruction_quality, 100);
        assert_eq!(f.wallet_hex, "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        assert_eq!(f.win_rate_bps, 6_667); // 2/3 → 6666.67 → 6667 (single ×10000, correct)
    }

    #[test]
    fn excludes_post_cutoff_closes() {
        let events = HashMap::new();
        let l = ledger(vec![
            closed("0xm1", dec!(0.50), 100, dec!(10.0), 3_600, 1_000),
            closed("0xm2", dec!(0.50), 100, dec!(99.0), 3_600, 9_999), // after cutoff
        ]);
        let f = extract_features(&l, 5_000, &events, &no_res(), 1, 0, 1, 1).unwrap();
        assert_eq!(f.closed_trades, 1);
        assert_eq!(f.total_pnl_usd, dec!(10.0));
        assert_eq!(f.win_rate_bps, 10_000); // the one in-window trade won
    }

    #[test]
    fn none_below_min_closed_trades() {
        let events = HashMap::new();
        let l = ledger(vec![closed("0xm1", dec!(0.5), 10, dec!(1.0), 60, 1_000)]);
        assert!(extract_features(&l, 10_000, &events, &no_res(), 5, 0, 1, 1).is_none());
    }

    #[test]
    fn none_when_no_closed_trades() {
        let events = HashMap::new();
        let l = ledger(vec![]);
        assert!(extract_features(&l, 10_000, &events, &no_res(), 1, 0, 1, 1).is_none());
    }

    #[test]
    fn distribution_moments_by_hand() {
        // Three distinct days; each trade cost = 0.50 × 100 = 50, so
        // return = pnl/50: pnl 5/10/15 → daily returns 0.1 / 0.2 / 0.3.
        let events = HashMap::new();
        let l = ledger(vec![
            closed("0xm1", dec!(0.50), 100, dec!(5.0), 60, 86_400),
            closed("0xm2", dec!(0.50), 100, dec!(10.0), 60, 172_800),
            closed("0xm3", dec!(0.50), 100, dec!(15.0), 60, 259_200),
        ]);
        let f = extract_features(&l, 10_000_000, &events, &no_res(), 1, 0, 1, 1).unwrap();

        assert_eq!(f.trading_days, 3);
        // mean = 0.2 → 2000 bps (exact)
        assert_eq!(f.mean_daily_return_bps, 2_000);
        // symmetric series → skewness 0 (exact)
        assert_eq!(f.skewness_bps, 0);
        // m4/std^4 = 1.5 exactly → excess kurtosis -1.5 → -15000 bps (exact)
        assert_eq!(f.excess_kurtosis_bps, -15_000);
        // sqrt-dependent: std≈0.08165 (816 bps), sharpe≈2.449 (24495), lcb≈0.1225 (1225)
        assert!(
            (f.std_daily_return_bps - 816).abs() <= 2,
            "std={}",
            f.std_daily_return_bps
        );
        assert!(
            (f.sharpe_bps - 24_495).abs() <= 5,
            "sharpe={}",
            f.sharpe_bps
        );
        assert!(
            (f.lcb_5pct_bps - 1_225).abs() <= 3,
            "lcb={}",
            f.lcb_5pct_bps
        );
    }

    #[test]
    fn single_day_has_zero_dispersion_and_lcb_equals_mean() {
        // All trades on one UTC day → 1 daily-return point → no dispersion.
        let events = HashMap::new();
        let l = ledger(vec![
            closed("0xm1", dec!(0.50), 100, dec!(5.0), 60, 1_000),
            closed("0xm2", dec!(0.50), 100, dec!(5.0), 60, 2_000),
        ]);
        let f = extract_features(&l, 10_000, &events, &no_res(), 1, 0, 1, 1).unwrap();
        assert_eq!(f.trading_days, 1);
        assert_eq!(f.std_daily_return_bps, 0);
        assert_eq!(f.sharpe_bps, 0);
        assert_eq!(f.skewness_bps, 0);
        assert_eq!(f.excess_kurtosis_bps, 0);
        // both trades same day: returns 0.1 + 0.1 = 0.2 → mean 2000 bps; lcb == mean
        assert_eq!(f.mean_daily_return_bps, 2_000);
        assert_eq!(f.lcb_5pct_bps, 2_000);
    }

    #[test]
    fn tiny_std_does_not_panic() {
        // Regression: when std is positive but tiny, std^3 underflows to Decimal::ZERO
        // (Decimal precision is 28 digits; std≈1e-10 → std^3≈1e-30, below the floor).
        // The fix: treat std3/std4 underflow as zero dispersion — skewness/kurtosis = 0.
        // Use many slightly-different returns across distinct days so std is non-zero but
        // extremely small (pnl differences of 1 sub-cent across 1000-contract positions).
        let events = HashMap::new();
        // 30 trades on 30 distinct days; entry 0.50, contracts 1_000_000, tiny pnl diffs.
        // daily return ≈ pnl / (0.50 × 1_000_000) = pnl / 500_000.
        // With pnl ranging 1e-7..1e-7 + 29e-9, daily returns ≈ 2e-13, std ≈ tiny.
        let trades: Vec<ClosedTrade> = (0u64..30)
            .map(|i| {
                let pnl = rust_decimal_macros::dec!(0.0000001) + Decimal::new(i as i64, 9); // adds i × 1e-9
                closed(
                    &format!("0xm{i}"),
                    dec!(0.50),
                    1_000_000,
                    pnl,
                    60,
                    (i as i64 + 1) * 86_400,
                )
            })
            .collect();
        let l = ledger(trades);
        // Must not panic. When std^3 underflows to Decimal::ZERO, skewness = 0.
        let f = extract_features(&l, i64::MAX, &events, &no_res(), 1, 0, 1, 1).unwrap();
        assert_eq!(f.trading_days, 30);
        assert_eq!(f.skewness_bps, 0);
        assert_eq!(f.excess_kurtosis_bps, 0);
    }

    // ─── New-feature unit tests (PR 1 candidate features) ────────────────────

    #[test]
    fn ev_mean_and_brier_by_hand_on_two_resolved_buys() {
        // Two resolved buys: 0.40 → won (o=1, edge=+0.60, (c-o)²=0.36)
        //                   0.70 → lost (o=0, edge=−0.70, (c-o)²=0.49)
        // EV mean = (0.60 + (−0.70)) / 2 = −0.05 → −500 bps.
        // Brier = (0.36 + 0.49) / 2 = 0.425 → 4250 bps.
        // ō = 0.5; two distinct deciles {4, 7} → ō_4 = 1, ō_7 = 0;
        // RES = (1·(1−0.5)² + 1·(0−0.5)²) / 2 = (0.25 + 0.25)/2 = 0.25 → 2500 bps.
        let events = HashMap::new();
        let mut resolutions = ResolutionIndex::new();
        resolutions.insert(market("0xwin"), res(0, 5_000));
        resolutions.insert(market("0xlose"), res(1, 5_000));
        let l = ledger(vec![
            closed("0xwin", dec!(0.40), 100, dec!(60.0), 60, 1_000),
            closed("0xlose", dec!(0.70), 100, dec!(-70.0), 60, 2_000),
        ]);
        let f = extract_features(&l, 10_000, &events, &resolutions, 1, 0, 1, 1).unwrap();
        assert_eq!(f.ev_mean_bps, -500);
        assert_eq!(f.brier_score_bps, 4_250);
        assert_eq!(f.brier_resolution_bps, 2_500);
    }

    #[test]
    fn bb_shrunk_edge_and_kelly_with_laplace_prior() {
        // Two resolved buys, both at c=0.40, one wins / one loses.
        // x = 1 win, n = 2; Laplace prior α=β=1 → p̂ = (1+1)/(2+2) = 0.5.
        // c̄ = 0.40; bb_edge = 0.5 − 0.40 = 0.10 → 1000 bps.
        // Kelly: b = (1−0.4)/0.4 = 1.5; f* = 0.10/0.60 = 0.16667;
        // g* = 0.5·ln(1 + 1.5·0.16667) + 0.5·ln(1 − 0.16667)
        //    = 0.5·ln(1.25) + 0.5·ln(0.83333)
        //    ≈ 0.5·0.22314 + 0.5·(−0.18232) = 0.02041 → 204 bps (±a few).
        let events = HashMap::new();
        let mut resolutions = ResolutionIndex::new();
        resolutions.insert(market("0xwin"), res(0, 5_000));
        resolutions.insert(market("0xlose"), res(1, 5_000));
        let l = ledger(vec![
            closed("0xwin", dec!(0.40), 100, dec!(60.0), 60, 1_000),
            closed("0xlose", dec!(0.40), 100, dec!(-40.0), 60, 2_000),
        ]);
        let f = extract_features(&l, 10_000, &events, &resolutions, 1, 0, 1, 1).unwrap();
        assert_eq!(f.bb_shrunk_edge_bps, 1_000);
        assert!(
            (f.kelly_log_growth_bps - 204).abs() <= 3,
            "kelly_g={}",
            f.kelly_log_growth_bps
        );
    }

    #[test]
    fn kelly_zero_when_no_edge() {
        // p̂ = 0.4 (no wins, 2 trades, Laplace → (0+1)/(2+2)=0.25... wait:
        // 0 wins, n=2, Laplace α=β=1 → p̂=(0+1)/4=0.25; c̄=0.40 → edge negative.
        let events = HashMap::new();
        let mut resolutions = ResolutionIndex::new();
        resolutions.insert(market("0xa"), res(1, 5_000));
        resolutions.insert(market("0xb"), res(1, 5_000));
        let l = ledger(vec![
            closed("0xa", dec!(0.40), 100, dec!(-40.0), 60, 1_000),
            closed("0xb", dec!(0.40), 100, dec!(-40.0), 60, 2_000),
        ]);
        let f = extract_features(&l, 10_000, &events, &resolutions, 1, 0, 1, 1).unwrap();
        // p̂ = 0.25, c̄ = 0.40 → bb_edge = −0.15 → −1500 bps. Kelly = 0 (f* ≤ 0).
        assert_eq!(f.bb_shrunk_edge_bps, -1_500);
        assert_eq!(f.kelly_log_growth_bps, 0);
    }

    #[test]
    fn unresolved_trades_skipped_in_per_bet_quality() {
        // 1 resolved win + 1 unresolved trade → per-bet stats see only the win.
        // EV = (1 − 0.50) / 1 = +0.50 → 5000 bps; Brier = (0.50−1)²/1 = 0.25 → 2500.
        let events = HashMap::new();
        let mut resolutions = ResolutionIndex::new();
        resolutions.insert(market("0xwin"), res(0, 5_000));
        let l = ledger(vec![
            closed("0xwin", dec!(0.50), 100, dec!(50.0), 60, 1_000),
            closed("0xunresolved", dec!(0.30), 100, dec!(-30.0), 60, 2_000),
        ]);
        let f = extract_features(&l, 10_000, &events, &resolutions, 1, 0, 1, 1).unwrap();
        // Sample-size fields unchanged by resolution coverage.
        assert_eq!(f.closed_trades, 2);
        assert_eq!(f.distinct_markets, 2);
        // Per-bet quality reflects only the resolved trade.
        assert_eq!(f.ev_mean_bps, 5_000);
        assert_eq!(f.brier_score_bps, 2_500);
    }

    #[test]
    fn brier_resolution_zero_when_all_entries_share_one_decile() {
        // All trades at c=0.50 → one bucket → ōₖ = ō → RES = 0.
        let events = HashMap::new();
        let mut resolutions = ResolutionIndex::new();
        resolutions.insert(market("0xa"), res(0, 5_000));
        resolutions.insert(market("0xb"), res(1, 5_000));
        let l = ledger(vec![
            closed("0xa", dec!(0.50), 100, dec!(50.0), 60, 1_000),
            closed("0xb", dec!(0.50), 100, dec!(-50.0), 60, 2_000),
        ]);
        let f = extract_features(&l, 10_000, &events, &resolutions, 1, 0, 1, 1).unwrap();
        assert_eq!(f.brier_resolution_bps, 0);
    }

    #[test]
    fn concentration_hhi_and_n_eff_by_hand() {
        // Two events with positive PnL roll-up: $10 + $10 → shares 0.5 each.
        // HHI = 0.25 + 0.25 = 0.5 → 5000 bps; N_eff = 1 / 0.5 = 2 → 20000 bps.
        // RPC = 1·0.5 + 2·0.5 = 1.5 → 15000 bps.
        let mut events = HashMap::new();
        events.insert("0xm1".to_owned(), "evtA".to_owned());
        events.insert("0xm2".to_owned(), "evtB".to_owned());
        let l = ledger(vec![
            closed("0xm1", dec!(0.50), 100, dec!(10.0), 60, 1_000),
            closed("0xm2", dec!(0.50), 100, dec!(10.0), 60, 2_000),
        ]);
        let f = extract_features(&l, 10_000, &events, &no_res(), 1, 0, 1, 1).unwrap();
        assert_eq!(f.concentration_hhi_bps, 5_000);
        assert_eq!(f.concentration_n_eff_bps, 20_000);
        assert_eq!(f.concentration_rpc_bps, 15_000);
    }

    #[test]
    fn concentration_zero_when_no_event_positive_pnl() {
        // All-negative PnL → no positive shares → all-zeros sentinel.
        let events = HashMap::new();
        let l = ledger(vec![
            closed("0xa", dec!(0.50), 100, dec!(-10.0), 60, 1_000),
            closed("0xb", dec!(0.50), 100, dec!(-20.0), 60, 2_000),
        ]);
        let f = extract_features(&l, 10_000, &events, &no_res(), 1, 0, 1, 1).unwrap();
        assert_eq!(f.concentration_hhi_bps, 0);
        assert_eq!(f.concentration_n_eff_bps, 0);
        assert_eq!(f.concentration_rpc_bps, 0);
    }

    #[test]
    fn first_entries_per_active_day_ratio() {
        // 3 distinct markets / 3 trading days = 1.0 → 10000 bps.
        let events = HashMap::new();
        let l = ledger(vec![
            closed("0xa", dec!(0.50), 100, dec!(5.0), 60, 86_400),
            closed("0xb", dec!(0.50), 100, dec!(5.0), 60, 172_800),
            closed("0xc", dec!(0.50), 100, dec!(5.0), 60, 259_200),
        ]);
        let f = extract_features(&l, 10_000_000, &events, &no_res(), 1, 0, 1, 1).unwrap();
        assert_eq!(f.first_entries_per_active_day_bps, 10_000);
    }

    #[test]
    fn median_first_entry_to_resolution_odd_and_even() {
        // First-entry timestamps and resolutions yield deltas [100, 200, 300].
        // Median (odd) = 200.
        let events = HashMap::new();
        let mut resolutions = ResolutionIndex::new();
        // closed() puts opened_at = closed_at − hold_secs; resolution_ts − opened_at = delta.
        // For "0xa" opened at 0, resolved at 100 → delta 100.
        // For "0xb" opened at 1000, resolved at 1200 → delta 200.
        // For "0xc" opened at 2000, resolved at 2300 → delta 300.
        resolutions.insert(market("0xa"), res(0, 100));
        resolutions.insert(market("0xb"), res(0, 1_200));
        resolutions.insert(market("0xc"), res(0, 2_300));
        let l = ledger(vec![
            // (market, entry, contracts, pnl, hold_secs, closed_at) →
            // opened_at = closed_at − hold_secs.
            closed("0xa", dec!(0.50), 100, dec!(5.0), 50, 50),
            closed("0xb", dec!(0.50), 100, dec!(5.0), 100, 1_100),
            closed("0xc", dec!(0.50), 100, dec!(5.0), 100, 2_100),
        ]);
        let f = extract_features(&l, 10_000, &events, &resolutions, 1, 0, 1, 1).unwrap();
        assert_eq!(f.median_first_entry_to_resolution_secs, 200);

        // Drop the third trade → deltas [100, 200], even-length median = 150.
        let l2 = ledger(vec![
            closed("0xa", dec!(0.50), 100, dec!(5.0), 50, 50),
            closed("0xb", dec!(0.50), 100, dec!(5.0), 100, 1_100),
        ]);
        let f2 = extract_features(&l2, 10_000, &events, &resolutions, 1, 0, 1, 1).unwrap();
        assert_eq!(f2.median_first_entry_to_resolution_secs, 150);
    }

    #[test]
    fn min_distinct_events_gate_rejects_below_threshold() {
        // 3 distinct events traded < gate of 10 → None.
        let mut events = HashMap::new();
        events.insert("0xa".to_owned(), "evt1".to_owned());
        events.insert("0xb".to_owned(), "evt2".to_owned());
        events.insert("0xc".to_owned(), "evt3".to_owned());
        let l = ledger(vec![
            closed("0xa", dec!(0.50), 100, dec!(5.0), 60, 1_000),
            closed("0xb", dec!(0.50), 100, dec!(5.0), 60, 2_000),
            closed("0xc", dec!(0.50), 100, dec!(5.0), 60, 3_000),
        ]);
        assert!(extract_features(&l, 10_000, &events, &no_res(), 1, 10, 1, 1).is_none());
        // Same data, gate of 3 → accepted (boundary == passes).
        let f = extract_features(&l, 10_000, &events, &no_res(), 1, 3, 1, 1);
        assert!(f.is_some());
    }
}
