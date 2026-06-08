//! P&L ledger, resolution ingestion, and dashboard for the paper copy-trader.
//!
//! - [`gamma`] — Gamma API client polling for market resolution.
//! - [`resolution`] — [`ResolutionStore`]: in-memory map backed by a JSON sidecar.
//! - [`pnl`] — Pure [`PnlLedger`]: derives a [`PortfolioSnapshot`] from DB + resolutions.
//! - [`valuation`] — Pure [`value_portfolio`]: realized/unrealized split + per-trade marks.
//! - [`dashboard`] — [`PortfolioSnapshot`] type and static HTML dashboard renderer.

pub mod dashboard;
pub mod gamma;
pub mod pnl;
pub mod resolution;
pub mod valuation;

pub use dashboard::{PortfolioSnapshot, TradeView, render_dashboard_html};
pub use gamma::{GammaError, GammaResolutionFetcher, MarketResolution, parse_outcome_prices};
pub use pnl::{PnlError, PnlLedger};
pub use resolution::{ResolutionStore, ResolutionStoreError, SettlementInfo};
pub use valuation::{FillOutcome, TradeValuation, ValuationOutput, value_portfolio};
