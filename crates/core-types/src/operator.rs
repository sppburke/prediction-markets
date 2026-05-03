use serde::{Deserialize, Serialize};

use crate::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct FundingHopCount(pub u8);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WalletAgeSeconds(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ClusterSize(pub u16);

/// Confidence (0–100) that an operator's funding graph has been fully reconstructed.
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
