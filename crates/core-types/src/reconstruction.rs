use serde::{Deserialize, Serialize};

use crate::Error;

/// Confidence (0–100) in the completeness of a reconstructed wallet trade ledger.
///
/// Carried on leader signals and watchlist entries; downstream sizing can gate
/// or discount low-confidence reconstructions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ReconstructionQuality(u8);

impl ReconstructionQuality {
    pub fn new(n: u8) -> Result<Self, Error> {
        if n > 100 {
            return Err(Error::OutOfRange {
                field: "ReconstructionQuality",
            });
        }
        Ok(Self(n))
    }

    pub fn get(self) -> u8 {
        self.0
    }
}
