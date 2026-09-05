//! P&L ledger, resolution ingestion, and portfolio-view types for the paper copy-trader.
//!
//! - [`resolution`] — [`ResolutionStore`]: in-memory map backed by the `settled_markets` SQLite table.
//! - [`pnl`] — Pure [`PnlLedger`]: derives a [`PortfolioSnapshot`] from DB + resolutions.
//! - [`valuation`] — Pure [`value_portfolio`]: realized/unrealized split + per-trade marks.
//! - [`dashboard`] — [`PortfolioSnapshot`] / [`TradeView`] types for the JSON endpoints.

pub mod aggregate;
pub mod dashboard;
pub mod pnl;
pub mod resolution;
pub mod valuation;

pub use aggregate::{ResolutionMathError, ResolutionPosition, aggregate_resolution_credit};
pub use dashboard::{PortfolioSnapshot, TradeView};
pub use pnl::{PnlError, PnlLedger};
pub use resolution::{ResolutionStore, ResolutionStoreError, SettlementInfo};
pub use valuation::{FillOutcome, TradeValuation, ValuationOutput, realized_edge, value_portfolio};
