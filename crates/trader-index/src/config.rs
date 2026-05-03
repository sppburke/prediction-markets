//! Configuration for ledger reconstruction and walk-forward ranking.
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

/// Configuration for [`build_watchlist`].
///
/// All defaults sourced from `docs/_GLOSSARY.md` (Watchlist sizes and Ranker eligibility tables).
///
/// [`build_watchlist`]: crate::ranker::build_watchlist
#[derive(Debug, Clone)]
pub struct RankerConfig {
    /// Number of calendar days in the active-tier look-back window.
    /// Default: 180 (`active_window_days` in `_GLOSSARY.md`).
    pub active_window_days: u32,
    /// Minimum closed trades in window for active-tier eligibility.
    /// Default: 60 (`active_min_closed_trades` in `_GLOSSARY.md`).
    pub active_min_closed_trades: u32,
    /// Minimum distinct markets traded for active-tier eligibility.
    /// Default: 30 (`active_min_distinct_markets` in `_GLOSSARY.md`).
    pub active_min_distinct_markets: u32,
    /// Number of calendar days in the incubator-tier look-back window.
    /// Default: 90 (`incubator_window_days` in `_GLOSSARY.md`).
    pub incubator_window_days: u32,
    /// Minimum closed trades in window for incubator-tier eligibility.
    /// Default: 20 (`incubator_min_closed_trades` in `_GLOSSARY.md`).
    pub incubator_min_closed_trades: u32,
    /// Minimum distinct markets traded for incubator-tier eligibility.
    /// Default: 10 (`incubator_min_distinct_markets` in `_GLOSSARY.md`).
    pub incubator_min_distinct_markets: u32,
    /// Maximum active leaders/operators in the watchlist.
    /// Default: 50 (`active_watchlist_size` in `_GLOSSARY.md`).
    pub active_watchlist_size: usize,
    /// Maximum incubator candidates in the watchlist.
    /// Default: 250 (`incubator_watchlist_size` in `_GLOSSARY.md`).
    pub incubator_watchlist_size: usize,
    /// Minimum `reconstruction_quality` (0–100) for any watchlist entry.
    /// Entries below this threshold are excluded even if they pass trade/market counts.
    /// Default: 60 (`research_only_quality_threshold` in `_GLOSSARY.md`).
    pub min_reconstruction_quality: u8,
}

impl Default for RankerConfig {
    fn default() -> Self {
        Self {
            active_window_days: 180,
            active_min_closed_trades: 60,
            active_min_distinct_markets: 30,
            incubator_window_days: 90,
            incubator_min_closed_trades: 20,
            incubator_min_distinct_markets: 10,
            active_watchlist_size: 50,
            incubator_watchlist_size: 250,
            min_reconstruction_quality: 60,
        }
    }
}
