use serde::{Deserialize, Serialize};

use crate::{Error, RoundingPolicy};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ContractQty(pub u64);

impl ContractQty {
    pub fn from_f64_rounding(v: f64, _policy: RoundingPolicy) -> Result<Self, Error> {
        if !v.is_finite() || v < 0.0 {
            return Err(Error::ConvError {
                message: format!("cannot convert f64 {v} to ContractQty"),
            });
        }
        Ok(Self(v.round() as u64))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Quantity(pub ContractQty);

impl Quantity {
    pub fn from_f64_rounding(v: f64, policy: RoundingPolicy) -> Result<Self, Error> {
        ContractQty::from_f64_rounding(v, policy).map(Quantity)
    }
}
