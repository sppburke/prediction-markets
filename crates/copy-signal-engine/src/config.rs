//! Configuration for the signal classifier.
//!
//! All defaults are canonical values from `docs/_GLOSSARY.md`.

use serde::{Deserialize, Serialize};

/// Thresholds governing trade classification.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalConfig {
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
            add_high_confidence_threshold_ppm: 700_000,
            exit_high_confidence_threshold_ppm: 700_000,
            near_close_remaining_pct: 10,
        }
    }
}
