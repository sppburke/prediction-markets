//! Pure, deterministic risk gate. Takes a typed `RiskSnapshot` (no live queries) and returns
//! `RiskApproved` or a `RiskBlock` variant with the reason. No I/O, no network, deterministic
//! from the snapshot.

pub mod block;
pub mod engine;
pub mod snapshot;

pub use block::RiskBlock;
pub use engine::{RiskDecision, evaluate_risk};
pub use snapshot::{RiskSnapshot, TradingMode};

use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive as _;

/// Clamp `contracts` so the notional cost stays within `cap_bps / 10_000` of `bankroll`.
///
/// Returns 0 when `cap_bps ≤ 0`, `bankroll ≤ 0`, or `price ≤ 0` (no trade can be sized).
/// When `available_bankroll < price`, returns 0 — no fractional contracts.
///
/// # Precondition
/// `price` must be the per-contract limit price (same value used in Kelly sizing).
pub fn clamp_contracts_to_cap(
    contracts: u64,
    price: Decimal,
    bankroll: Decimal,
    cap_bps: i32,
) -> u64 {
    if cap_bps <= 0 || bankroll <= Decimal::ZERO || price <= Decimal::ZERO {
        return 0;
    }
    let cap_usd = bankroll * Decimal::from(cap_bps) / Decimal::from(10_000i32);
    let max_contracts_dec = (cap_usd / price).floor();
    let max_contracts = max_contracts_dec.to_u64().unwrap_or(u64::MAX);
    contracts.min(max_contracts)
}
