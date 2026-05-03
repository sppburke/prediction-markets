//! Configuration for ledger reconstruction.
//!
//! All defaults sourced from `docs/_GLOSSARY.md`.

/// Configuration for [`build_trader_ledgers`].
///
/// [`build_trader_ledgers`]: crate::reconstruction::build_trader_ledgers
#[derive(Debug, Clone)]
pub struct LedgerConfig {
    /// Minimum `confidence_ppm` from `OperatorIdentity` required to annotate a ledger
    /// with an `operator_id`. Default: 850_000 (= 0.85, from `_GLOSSARY.md`
    /// `funder_root_min_confidence_ppm`).
    pub operator_min_confidence_ppm: u32,
    /// `reconstruction_quality` below this value marks the ledger research-only
    /// (ineligible for the watchlist). Default: 60 (from `_GLOSSARY.md`).
    pub research_only_quality_threshold: u8,
    /// Maximum closed trades for a wallet to still be considered "fresh".
    /// Default: 2 (from `_GLOSSARY.md` `fresh_wallet_max_closed_trades`).
    pub fresh_wallet_max_closed_trades: u32,
}

impl Default for LedgerConfig {
    fn default() -> Self {
        Self {
            operator_min_confidence_ppm: 850_000,
            research_only_quality_threshold: 60,
            fresh_wallet_max_closed_trades: 2,
        }
    }
}
