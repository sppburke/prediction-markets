//! Pure P&L ledger — derives a [`PortfolioSnapshot`] from paper-state + resolutions.
//!
//! No I/O in this module. All DB reads happen via `PaperStateDb` and resolution data
//! is supplied by `ResolutionStore`. The caller is responsible for applying resolution
//! credits to the bankroll before calling `snapshot`.

use std::collections::HashMap;

use pe_core_types::MarketId;
use pe_paper_state::{PaperPositionRow, PaperStateDb};
use rust_decimal::Decimal;

use crate::dashboard::PortfolioSnapshot;
use crate::resolution::ResolutionStore;
use crate::valuation::value_portfolio;

/// Derives [`PortfolioSnapshot`] from DB state + settled resolutions.
pub struct PnlLedger;

impl PnlLedger {
    /// Compute the current portfolio snapshot.
    ///
    /// # Precondition
    /// Resolution credits must already be applied to the bankroll in `paper_state`
    /// (via [`PaperStateDb::credit_bankroll`]) before calling this. The snapshot
    /// reflects the bankroll as stored.
    ///
    /// `open_mids` carries current Polymarket mids (per `outcome_id`) for open
    /// markets so open positions are marked to market; settled markets are valued
    /// from the resolution store. Pass an empty map to value open positions at $0.
    pub fn snapshot(
        paper_state: &PaperStateDb,
        resolution_store: &ResolutionStore,
        initial_bankroll: Decimal,
        open_mids: &HashMap<MarketId, Vec<Decimal>>,
    ) -> Result<PortfolioSnapshot, PnlError> {
        let current_bankroll = paper_state.bankroll()?.unwrap_or(Decimal::ZERO);
        let positions = paper_state.paper_positions()?;
        let fills = paper_state.list_fills()?;

        // One valuation pass derives realized/unrealized, the open-position count
        // (settled markets excluded), and per-fill marks; the summary card and the
        // per-trade table cannot drift because both read this single result.
        let valuation = value_portfolio(
            &fills,
            &positions,
            resolution_store,
            open_mids,
            current_bankroll,
            initial_bankroll,
        );

        Ok(PortfolioSnapshot::from_valuation(
            &valuation,
            current_bankroll,
            initial_bankroll,
            resolution_store.total_credits(),
            resolution_store.settled_count(),
            fills.len(),
        ))
    }

    /// Compute the bankroll credit for a resolved market from the provided position rows.
    ///
    /// Formula per position: `(long_contracts - short_contracts) × outcome_prices[outcome_id]`.
    /// Clamped to zero so a net-short position never debits the bankroll.
    pub fn resolution_credit(
        positions: &[PaperPositionRow],
        outcome_prices: &[Decimal],
    ) -> Decimal {
        let credit: Decimal = positions
            .iter()
            .map(|p| {
                let idx = usize::from(p.outcome_id.0);
                let price = outcome_prices.get(idx).copied().unwrap_or(Decimal::ZERO);
                let long = Decimal::from(p.long_contracts);
                let short = Decimal::from(p.short_contracts);
                (long - short) * price
            })
            .fold(Decimal::ZERO, |acc, v| acc.checked_add(v).unwrap_or(acc));
        credit.max(Decimal::ZERO)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PnlError {
    #[error("paper-state: {0}")]
    PaperState(#[from] pe_paper_state::PaperStateError),
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use pe_core_types::{MarketId, OutcomeId, VenueMarketId};
    use rust_decimal_macros::dec;

    fn market() -> MarketId {
        MarketId(VenueMarketId("0x1".to_string()))
    }

    fn pos(outcome: u16, long: u64, short: u64) -> PaperPositionRow {
        PaperPositionRow {
            market_id: market(),
            outcome_id: OutcomeId(outcome),
            long_contracts: long,
            short_contracts: short,
        }
    }

    #[test]
    fn resolution_credit_yes_wins() {
        let credit = PnlLedger::resolution_credit(&[pos(0, 10, 0)], &[dec!(1), dec!(0)]);
        assert_eq!(credit, dec!(10));
    }

    #[test]
    fn resolution_credit_yes_loses() {
        let credit = PnlLedger::resolution_credit(&[pos(0, 10, 0)], &[dec!(0), dec!(1)]);
        assert_eq!(credit, Decimal::ZERO);
    }

    #[test]
    fn resolution_credit_clamped_to_zero() {
        // net short: long=0, short=5 → clamped to 0
        let credit = PnlLedger::resolution_credit(&[pos(0, 0, 5)], &[dec!(1), dec!(0)]);
        assert_eq!(credit, Decimal::ZERO);
    }

    #[test]
    fn resolution_credit_multi_outcome() {
        // YES long=5, NO long=3, YES wins
        let positions = vec![pos(0, 5, 0), pos(1, 3, 0)];
        let credit = PnlLedger::resolution_credit(&positions, &[dec!(1), dec!(0)]);
        assert_eq!(credit, dec!(5)); // only YES bucket pays
    }
}
