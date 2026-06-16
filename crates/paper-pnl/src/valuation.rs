//! Pure portfolio valuation — the single source of truth for the dashboard's
//! realized/unrealized P&L split and per-trade marks.
//!
//! No I/O. The service tier fetches live mids and passes them in as data; the
//! settlement source is the in-memory [`ResolutionStore`]. Open vs settled is
//! decided **solely** by [`ResolutionStore::is_settled`] — never by any cache —
//! so the summary card and the per-trade table, both derived from one
//! [`value_portfolio`] call, cannot drift.
//!
//! Accounting identities (`T = current_bankroll − initial_bankroll`):
//! - `open_cost`           = Σ over open fills of `fill_price × contracts` (buy +, sell −)
//! - `open_market_value`   = Σ over open positions of `(long − short) × mid[outcome_id]`
//! - `realized_pnl`        = `T + open_cost`
//! - `unrealized_pnl`      = `open_market_value − open_cost`
//! - displayed total       = `realized_pnl + unrealized_pnl = T + open_market_value`
//!
//! An open position with no live mid contributes 0 to `open_market_value` and is
//! counted in `open_positions_missing_price`; its per-row mark is `None`.

use std::collections::{HashMap, HashSet};

use pe_core_types::{MarketId, Side};
use pe_paper_state::{FillRow, PaperPositionRow};
use rust_decimal::Decimal;

use crate::resolution::{ResolutionStore, SettlementInfo};

/// Settled-fill outcome, for the per-trade display.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FillOutcome {
    /// Market settled and this fill realized a positive P&L.
    Won,
    /// Market settled and this fill realized a non-positive P&L.
    Lost,
    /// Market still open (not settled).
    Open,
}

/// Per-fill valuation, parallel to the input `fills` slice (same order and length).
#[derive(Debug, Clone)]
pub struct TradeValuation {
    pub outcome: FillOutcome,
    /// Realized P&L for a settled fill; `None` while the market is open.
    pub realized_pnl: Option<Decimal>,
    /// Current mark: the resolved price for a settled fill, the live mid for an
    /// open one, or `None` if an open market has no mid yet.
    pub current_mid: Option<Decimal>,
}

/// Aggregate + per-fill output of [`value_portfolio`].
#[derive(Debug, Clone)]
pub struct ValuationOutput {
    /// `T + open_cost` — P&L from settled markets (bankroll-derived).
    pub realized_pnl: Decimal,
    /// `open_market_value − open_cost` — mark-to-market of open positions.
    pub unrealized_pnl: Decimal,
    /// `Σ (long − short) × mid` over priced live open positions.
    pub open_market_value: Decimal,
    /// `Σ fill_price × contracts` (buy +, sell −) over fills backing a live open
    /// position.
    pub open_cost: Decimal,
    /// Live open positions with no live mid; valued at 0 in the aggregate, marked
    /// `None` per row.
    pub open_positions_missing_price: usize,
    /// Count of genuinely-open positions (non-zero net contracts, market unsettled).
    pub open_position_count: usize,
    /// `(T + all_fills_cost) − recorded total_credits` — bankroll-implied settlement
    /// credits vs. the recorded total. ~0 in normal operation; the zero-floor
    /// bankroll debit clamp is a known legitimate source of drift. Callers log (not
    /// panic) when it exceeds tolerance.
    pub reconciliation_drift: Decimal,
    /// Per-fill valuations, parallel to the input `fills` slice.
    pub trades: Vec<TradeValuation>,
}

/// Value the portfolio from DB state + live mids. Pure and deterministic.
///
/// A `(market, outcome)` is a **live open position** when it has a net position
/// (`long ≠ short`) on an unsettled market. `open_market_value` marks those
/// positions; `open_cost` is the signed cost of the fills that back them. Gating
/// `open_cost` on *live* positions (not merely unsettled markets) means a position
/// fully closed before settlement — a buy then equal sell — realizes its round-trip
/// P&L (it stays in the bankroll delta `T`) instead of leaking into unrealized.
///
/// Cost-basis limitation: a *partially* closed position (e.g. bought 100, sold 40,
/// 60 held) still folds all its fills into `open_cost`, so the sold portion's
/// realized P&L is approximated as unrealized. `PaperPositionRow` carries no
/// cost basis, so an exact split is out of scope; the displayed total stays correct.
pub fn value_portfolio(
    fills: &[FillRow],
    positions: &[PaperPositionRow],
    resolutions: &ResolutionStore,
    open_mids: &HashMap<MarketId, Vec<Decimal>>,
    current_bankroll: Decimal,
    initial_bankroll: Decimal,
) -> ValuationOutput {
    let t = current_bankroll - initial_bankroll;

    // open_market_value: (long − short) × mid over live open positions. Record which
    // (market, outcome) are live so open_cost can be gated on the same set. An
    // unpriced live position contributes 0 to the value and is counted.
    let mut open_market_value = Decimal::ZERO;
    let mut open_positions_missing_price = 0usize;
    let mut open_position_count = 0usize;
    let mut live: HashSet<(MarketId, u16)> = HashSet::new();
    for p in positions {
        if (p.long_contracts == 0 && p.short_contracts == 0) || resolutions.is_settled(&p.market_id)
        {
            continue;
        }
        open_position_count += 1;
        live.insert((p.market_id.clone(), p.outcome_id.0));
        match mid_for(open_mids, &p.market_id, p.outcome_id.0) {
            Some(mid) => {
                let net = Decimal::from(p.long_contracts) - Decimal::from(p.short_contracts);
                let v = net.checked_mul(mid).unwrap_or(Decimal::ZERO);
                open_market_value = open_market_value
                    .checked_add(v)
                    .unwrap_or(open_market_value);
            }
            None => open_positions_missing_price += 1,
        }
    }

    // open_cost: signed cost of fills backing a still-live open position (buy +,
    // sell −). all_fills_cost: every fill, for the bankroll reconciliation.
    let mut open_cost = Decimal::ZERO;
    let mut all_fills_cost = Decimal::ZERO;
    for f in fills {
        let signed = signed_cost(f);
        all_fills_cost = all_fills_cost.checked_add(signed).unwrap_or(all_fills_cost);
        if live.contains(&(f.market_id.clone(), f.outcome_id.0)) {
            open_cost = open_cost.checked_add(signed).unwrap_or(open_cost);
        }
    }

    let realized_pnl = t.checked_add(open_cost).unwrap_or(t);
    let unrealized_pnl = open_market_value
        .checked_sub(open_cost)
        .unwrap_or(open_market_value);

    // Reconciliation (split-independent): the bankroll-implied settlement credits
    // (`T + all_fills_cost`) should equal the recorded `total_credits`. Drift flags a
    // bankroll↔settlement skew (e.g. the zero-floor debit clamp); logged, not panicked.
    let implied_credits = t.checked_add(all_fills_cost).unwrap_or(t);
    let reconciliation_drift = implied_credits
        .checked_sub(resolutions.total_credits())
        .unwrap_or(Decimal::ZERO);

    let trades = fills
        .iter()
        .map(|f| value_fill(f, resolutions, open_mids))
        .collect();

    ValuationOutput {
        realized_pnl,
        unrealized_pnl,
        open_market_value,
        open_cost,
        open_positions_missing_price,
        open_position_count,
        reconciliation_drift,
        trades,
    }
}

/// Signed cash flow of a fill: buys debit (`+`), sells credit (`−`).
fn signed_cost(f: &FillRow) -> Decimal {
    let notional = f
        .fill_price
        .0
        .checked_mul(Decimal::from(f.contracts))
        .unwrap_or(Decimal::ZERO);
    match f.side {
        Side::Buy => notional,
        Side::Sell => -notional,
    }
}

/// Mid for `(market, outcome_id)`, if `open_mids` carries it.
fn mid_for(
    open_mids: &HashMap<MarketId, Vec<Decimal>>,
    market: &MarketId,
    outcome_id: u16,
) -> Option<Decimal> {
    open_mids
        .get(market)
        .and_then(|prices| prices.get(usize::from(outcome_id)).copied())
}

/// Realized P&L of a single **settled** fill — `side_sign × (resolved − fill_price)
/// × contracts`, where `resolved` is the settled price of the fill's outcome (`0` if
/// the outcome index is out of range, matching the prior inline behaviour).
///
/// This is the settled branch of [`value_fill`]; `value_fill` calls it so the
/// dashboard's realized P&L and the underperformance-knockout statistic (#350 WS1)
/// share a single definition rather than two formulas that can silently drift.
/// Returns `0` on any decimal-overflow step (the same saturating behaviour
/// `value_fill` had).
///
/// The per-share edge — `realized_edge / contracts = side_sign × (resolved −
/// fill_price)` — is bounded in `[-1, 1]` for binary settlement (`resolved ∈ {0,1}`,
/// `fill_price ∈ [0,1]`); the demotion statistic relies on that bound.
pub fn realized_edge(f: &FillRow, info: &SettlementInfo) -> Decimal {
    let resolved = info
        .outcome_prices
        .get(usize::from(f.outcome_id.0))
        .copied()
        .unwrap_or(Decimal::ZERO);
    let diff = resolved
        .checked_sub(f.fill_price.0)
        .unwrap_or(Decimal::ZERO);
    let magnitude = diff
        .checked_mul(Decimal::from(f.contracts))
        .unwrap_or(Decimal::ZERO);
    match f.side {
        Side::Buy => magnitude,
        Side::Sell => -magnitude,
    }
}

/// Value a single fill: settled → realized P&L + Won/Lost from the resolved price;
/// open → the live mid as the current mark (or `None` if unpriced).
fn value_fill(
    f: &FillRow,
    resolutions: &ResolutionStore,
    open_mids: &HashMap<MarketId, Vec<Decimal>>,
) -> TradeValuation {
    if let Some(info) = resolutions.settlement_info(&f.market_id) {
        let resolved = info
            .outcome_prices
            .get(usize::from(f.outcome_id.0))
            .copied()
            .unwrap_or(Decimal::ZERO);
        let realized = realized_edge(f, &info);
        let outcome = if realized > Decimal::ZERO {
            FillOutcome::Won
        } else {
            FillOutcome::Lost
        };
        TradeValuation {
            outcome,
            realized_pnl: Some(realized),
            current_mid: Some(resolved),
        }
    } else {
        TradeValuation {
            outcome: FillOutcome::Open,
            realized_pnl: None,
            current_mid: mid_for(open_mids, &f.market_id, f.outcome_id.0),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use pe_core_types::{MarketId, OutcomeId, Price, VenueMarketId};
    use rust_decimal_macros::dec;

    fn mid(s: &str) -> MarketId {
        MarketId(VenueMarketId(s.to_string()))
    }

    fn fill(market: &str, outcome: u16, side: Side, contracts: u64, price: Decimal) -> FillRow {
        FillRow {
            idempotency_key: format!("wf|0xL|0xS|{market}|{outcome}|buy|1700000000"),
            market_id: mid(market),
            outcome_id: OutcomeId(outcome),
            side,
            contracts,
            fill_price: Price(price),
            event_seq: 1,
        }
    }

    fn pos(market: &str, outcome: u16, long: u64, short: u64) -> PaperPositionRow {
        PaperPositionRow {
            market_id: mid(market),
            outcome_id: OutcomeId(outcome),
            long_contracts: long,
            short_contracts: short,
        }
    }

    /// A store seeded with the given settled markets (price arrays + recorded credit).
    /// The store owns its SQLite-backed `PaperStateDb` (held in the returned `TempDir`).
    fn store_with(
        settled: &[(&str, Vec<Decimal>, Decimal)],
    ) -> (tempfile::TempDir, ResolutionStore) {
        let dir = tempfile::tempdir().unwrap();
        let db = std::sync::Arc::new(
            pe_paper_state::PaperStateDb::open(&dir.path().join("paper_state.db")).unwrap(),
        );
        let mut store = ResolutionStore::load(db).unwrap();
        for (m, prices, credit) in settled {
            store
                .mark_settled(mid(m), prices.clone(), *credit, 1_700_000_000)
                .unwrap();
        }
        (dir, store)
    }

    #[test]
    fn open_position_with_mid_marks_to_market() {
        // Bought 100 of YES @ 0.40 (cost 40). Bankroll debited 40 → T = −40.
        // Mid now 0.55 → MV = 55, unrealized = 55 − 40 = +15, realized = −40 + 40 = 0.
        let (_d, store) = store_with(&[]);
        let fills = vec![fill("0xm", 0, Side::Buy, 100, dec!(0.40))];
        let positions = vec![pos("0xm", 0, 100, 0)];
        let mut mids = HashMap::new();
        mids.insert(mid("0xm"), vec![dec!(0.55), dec!(0.45)]);

        let v = value_portfolio(&fills, &positions, &store, &mids, dec!(9960), dec!(10000));
        assert_eq!(v.open_cost, dec!(40));
        assert_eq!(v.open_market_value, dec!(55));
        assert_eq!(v.unrealized_pnl, dec!(15));
        assert_eq!(v.realized_pnl, dec!(0));
        assert_eq!(v.open_positions_missing_price, 0);
        assert_eq!(v.open_position_count, 1);
        assert_eq!(v.trades[0].outcome, FillOutcome::Open);
        assert_eq!(v.trades[0].current_mid, Some(dec!(0.55)));
        assert_eq!(v.trades[0].realized_pnl, None);
        assert_eq!(v.reconciliation_drift, dec!(0));
    }

    #[test]
    fn settled_winner_realizes_profit() {
        // Bought 100 YES @ 0.40 (cost 40); YES resolved → credit 100. Bankroll
        // 10000 − 40 + 100 = 10060 → T = 60. realized = T + open_cost(0) = 60.
        let (_d, store) = store_with(&[("0xm", vec![dec!(1), dec!(0)], dec!(100))]);
        let fills = vec![fill("0xm", 0, Side::Buy, 100, dec!(0.40))];
        let positions = vec![pos("0xm", 0, 100, 0)];

        let v = value_portfolio(
            &fills,
            &positions,
            &store,
            &HashMap::new(),
            dec!(10060),
            dec!(10000),
        );
        assert_eq!(v.open_cost, dec!(0)); // settled, excluded from open
        assert_eq!(v.realized_pnl, dec!(60));
        assert_eq!(v.unrealized_pnl, dec!(0));
        assert_eq!(v.open_position_count, 0);
        assert_eq!(v.trades[0].outcome, FillOutcome::Won);
        assert_eq!(v.trades[0].realized_pnl, Some(dec!(60))); // (1−0.40)×100
        assert_eq!(v.reconciliation_drift, dec!(0));
    }

    #[test]
    fn settled_loser_realizes_loss_without_double_count() {
        // Bought 100 NO @ 0.30 (cost 30); YES resolved → NO worthless, credit 0.
        // Bankroll 10000 − 30 = 9970 → T = −30. realized = −30, no double count.
        let (_d, store) = store_with(&[("0xm", vec![dec!(1), dec!(0)], dec!(0))]);
        let fills = vec![fill("0xm", 1, Side::Buy, 100, dec!(0.30))];
        let positions = vec![pos("0xm", 1, 100, 0)];

        let v = value_portfolio(
            &fills,
            &positions,
            &store,
            &HashMap::new(),
            dec!(9970),
            dec!(10000),
        );
        assert_eq!(v.realized_pnl, dec!(-30));
        assert_eq!(v.unrealized_pnl, dec!(0));
        assert_eq!(v.trades[0].outcome, FillOutcome::Lost);
        assert_eq!(v.trades[0].realized_pnl, Some(dec!(-30))); // (0−0.30)×100
        assert_eq!(v.reconciliation_drift, dec!(0));
    }

    #[test]
    fn open_position_missing_mid_is_counted_and_valued_zero() {
        let (_d, store) = store_with(&[]);
        let fills = vec![fill("0xm", 0, Side::Buy, 100, dec!(0.40))];
        let positions = vec![pos("0xm", 0, 100, 0)];
        // No mid supplied for 0xm.
        let v = value_portfolio(
            &fills,
            &positions,
            &store,
            &HashMap::new(),
            dec!(9960),
            dec!(10000),
        );
        assert_eq!(v.open_positions_missing_price, 1);
        assert_eq!(v.open_market_value, dec!(0));
        assert_eq!(v.unrealized_pnl, dec!(-40)); // 0 − open_cost(40)
        assert_eq!(v.trades[0].current_mid, None);
    }

    #[test]
    fn mixed_open_and_settled_reconciles() {
        // Settled winner: bought 100 YES @ 0.40 → credit 100.
        // Open: bought 50 YES @ 0.50 (cost 25), mid 0.60 → MV 30.
        // Bankroll = 10000 − 40 (buy1) + 100 (credit) − 25 (buy2) = 10035 → T = 35.
        // open_cost = 25, realized = 35 + 25 = 60, unrealized = 30 − 25 = 5.
        let (_d, store) = store_with(&[("0xa", vec![dec!(1), dec!(0)], dec!(100))]);
        let fills = vec![
            fill("0xa", 0, Side::Buy, 100, dec!(0.40)),
            fill("0xb", 0, Side::Buy, 50, dec!(0.50)),
        ];
        let positions = vec![pos("0xa", 0, 100, 0), pos("0xb", 0, 50, 0)];
        let mut mids = HashMap::new();
        mids.insert(mid("0xb"), vec![dec!(0.60), dec!(0.40)]);

        let v = value_portfolio(&fills, &positions, &store, &mids, dec!(10035), dec!(10000));
        assert_eq!(v.open_cost, dec!(25));
        assert_eq!(v.realized_pnl, dec!(60));
        assert_eq!(v.unrealized_pnl, dec!(5));
        assert_eq!(v.realized_pnl + v.unrealized_pnl, dec!(65)); // displayed total
        assert_eq!(v.open_position_count, 1);
        assert_eq!(v.reconciliation_drift, dec!(0));
    }

    #[test]
    fn closed_unsettled_roundtrip_realizes_pnl() {
        // BUY 100 @0.40 then SELL 100 @0.55 on an OPEN market → position nets to 0.
        // The +15 round-trip gain is realized (locked in), not unrealized.
        let (_d, store) = store_with(&[]);
        let fills = vec![
            fill("0xm", 0, Side::Buy, 100, dec!(0.40)),
            fill("0xm", 0, Side::Sell, 100, dec!(0.55)),
        ];
        let positions = vec![pos("0xm", 0, 0, 0)]; // fully closed
        // Bankroll: 10000 − 40 + 55 = 10015 → T = 15.
        let v = value_portfolio(
            &fills,
            &positions,
            &store,
            &HashMap::new(),
            dec!(10015),
            dec!(10000),
        );
        assert_eq!(
            v.open_cost,
            dec!(0),
            "a closed position contributes no open cost"
        );
        assert_eq!(
            v.realized_pnl,
            dec!(15),
            "round-trip gain is realized, not unrealized"
        );
        assert_eq!(v.unrealized_pnl, dec!(0));
        assert_eq!(v.open_position_count, 0);
        assert_eq!(v.reconciliation_drift, dec!(0));
    }

    #[test]
    fn reconciliation_drift_flags_credit_mismatch() {
        // Bankroll reflects a 100 credit, but the store records only 90 → drift +10.
        let (_d, store) = store_with(&[("0xm", vec![dec!(1), dec!(0)], dec!(90))]);
        let fills = vec![fill("0xm", 0, Side::Buy, 100, dec!(0.40))];
        let positions = vec![pos("0xm", 0, 100, 0)];
        // Bankroll = 10000 − 40 + 100 = 10060 → realized = 60; recorded = 90 − 40 = 50.
        let v = value_portfolio(
            &fills,
            &positions,
            &store,
            &HashMap::new(),
            dec!(10060),
            dec!(10000),
        );
        assert_eq!(v.reconciliation_drift, dec!(10));
    }

    #[test]
    fn realized_edge_matches_value_fill_realized_pnl() {
        // Structural-reuse guarantee (#350 WS1): value_fill calls realized_edge, so
        // for any settled fill the standalone primitive equals the per-trade realized
        // P&L the dashboard shows. A drift between the two would be a logic bug.
        let (_d, store) = store_with(&[("0xm", vec![dec!(1), dec!(0)], dec!(100))]);
        let info = store.settlement_info(&mid("0xm")).unwrap();

        // BUY YES @ 0.40, YES resolves to 1 → (1 − 0.40) × 100 = +60.
        let buy = fill("0xm", 0, Side::Buy, 100, dec!(0.40));
        let via_value_fill = value_portfolio(
            std::slice::from_ref(&buy),
            &[pos("0xm", 0, 100, 0)],
            &store,
            &HashMap::new(),
            dec!(10060),
            dec!(10000),
        )
        .trades[0]
            .realized_pnl
            .unwrap();
        assert_eq!(realized_edge(&buy, &info), via_value_fill);
        assert_eq!(realized_edge(&buy, &info), dec!(60));

        // SELL flips the sign: (1 − 0.40) × 100, negated → −60.
        let sell = fill("0xm", 0, Side::Sell, 100, dec!(0.40));
        assert_eq!(realized_edge(&sell, &info), dec!(-60));

        // Out-of-range outcome index resolves to price 0 (no panic, matches inline).
        let oob = fill("0xm", 9, Side::Buy, 100, dec!(0.40));
        assert_eq!(realized_edge(&oob, &info), dec!(-40)); // (0 − 0.40) × 100
    }
}
