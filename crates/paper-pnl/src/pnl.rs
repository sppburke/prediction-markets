//! Pure P&L ledger — derives a [`PortfolioSnapshot`] from paper-state + resolutions.
//!
//! No I/O in this module. All DB reads happen via `PaperStateDb` and resolution data
//! is supplied by `ResolutionStore`. The caller is responsible for applying resolution
//! credits to the bankroll before calling `snapshot`.

use std::collections::HashMap;

use pe_core_types::MarketId;
use pe_paper_state::PaperStateDb;
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
}

#[derive(Debug, thiserror::Error)]
pub enum PnlError {
    #[error("paper-state: {0}")]
    PaperState(#[from] pe_paper_state::PaperStateError),
}
