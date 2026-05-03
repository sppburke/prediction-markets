//! Configuration for the signal classifier.
//!
//! All defaults are canonical values from `docs/_GLOSSARY.md`.

use serde::{Deserialize, Serialize};

/// Thresholds governing trade classification and signal-kind detection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalConfig {
    /// Wallet still counts as "fresh" if closed-trade count ≤ this. Default: 2.
    pub fresh_wallet_max_closed_trades: u32,
    /// Maximum wallet age in seconds to be considered "fresh". Default: 1_209_600 (14 days).
    pub fresh_wallet_max_age_seconds: u32,
    /// Minimum position notional in USD to emit a `FreshWalletFirstTrade` signal. Default: 50.
    pub inherited_prior_min_position_usd: u32,
    /// Minimum number of coordinating wallets for cluster detection. Default: 3.
    pub cluster_coord_min_members: u32,
    /// Window in seconds during which coordinating entries count. Default: 300.
    pub cluster_coord_window_seconds: u32,
    /// Minimum aggregate notional in USD across coordinating wallets. Default: 1_000.
    pub cluster_coord_min_aggregate_usd: u32,
    /// `action_confidence_ppm` must be ≥ this to copy an `Add` trade. Default: 700_000.
    pub add_high_confidence_threshold_ppm: u32,
    /// `action_confidence_ppm` must be ≥ this to copy a `Trim`/`Exit` trade. Default: 700_000.
    pub exit_high_confidence_threshold_ppm: u32,
    /// Fraction of the original position remaining after a close that flips it from
    /// `Trim` to `Exit`. Default: 10 (10%).
    pub near_close_remaining_pct: u8,
}

impl Default for SignalConfig {
    fn default() -> Self {
        Self {
            fresh_wallet_max_closed_trades: 2,
            fresh_wallet_max_age_seconds: 1_209_600,
            inherited_prior_min_position_usd: 50,
            cluster_coord_min_members: 3,
            cluster_coord_window_seconds: 300,
            cluster_coord_min_aggregate_usd: 1_000,
            add_high_confidence_threshold_ppm: 700_000,
            exit_high_confidence_threshold_ppm: 700_000,
            near_close_remaining_pct: 10,
        }
    }
}
