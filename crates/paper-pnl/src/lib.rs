//! P&L ledger, resolution ingestion, and dashboard for the paper copy-trader.
//!
//! - [`gamma`] — Gamma API client polling for market resolution.
//! - [`resolution`] — [`ResolutionStore`]: in-memory map backed by a JSON sidecar.
//! - [`pnl`] — Pure [`PnlLedger`]: derives a [`PortfolioSnapshot`] from DB + resolutions.
//! - [`dashboard`] — [`PortfolioSnapshot`] type and static HTML dashboard renderer.

pub mod dashboard;
pub mod gamma;
pub mod pnl;
pub mod resolution;

pub use dashboard::{PortfolioSnapshot, render_dashboard_html};
pub use gamma::{GammaError, GammaResolutionFetcher, MarketResolution};
pub use pnl::{PnlError, PnlLedger};
pub use resolution::{ResolutionStore, ResolutionStoreError};
