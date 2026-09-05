//! Canonical prepared-order economics shared by paper, live, and replay (#545).

use pe_core_types::{
    CollateralAmount, KellyFraction, PolymarketConditionId, PolymarketTokenId, Price, Probability,
    ShareAmount, Side,
};
use pe_event_log::AppendReceipt;
use pe_risk_engine::{RiskBlock, RiskSnapshot};
use pe_venue_polymarket::{
    CompactFeeSchedule, FeeError, LadderError, LadderPlan, fee_reserve, taker_fee,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::live_executor::LiveAdmissionArtifact;
use crate::live_journal::{
    LadderPlanAudit, LiveAdmissionArtifactAudit, LiveJournalError, hash_serializable,
};

pub const ECONOMIC_PREPARED_VERSION: u16 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketSelection {
    pub condition_id: PolymarketConditionId,
    pub outcome_index: u8,
    pub token_id: PolymarketTokenId,
    pub side: Side,
    pub market_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "mode", deny_unknown_fields)]
pub enum SizingModeAudit {
    Kelly {
        fraction: KellyFraction,
        probability: Probability,
    },
    Dollar {
        usd: Decimal,
    },
    Contract {
        contracts: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SizingAudit {
    pub mode: SizingModeAudit,
    pub budget: CollateralAmount,
    pub principal: CollateralAmount,
    pub minimum_shares: ShareAmount,
    pub expected_shares: ShareAmount,
    pub expected_vwap: Price,
    pub all_in_price: Price,
    pub slippage_rate: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeeAudit {
    pub schedule: CompactFeeSchedule,
    pub expected_fee: CollateralAmount,
    pub reserve: CollateralAmount,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "decision", deny_unknown_fields)]
pub enum RiskDecisionAudit {
    Approved,
    Blocked { reason: RiskBlock },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RiskAudit {
    pub snapshot: RiskSnapshot,
    pub decision: RiskDecisionAudit,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BalanceAudit {
    pub cash_before: CollateralAmount,
    pub worst_case_debit: CollateralAmount,
    pub price_impact_cap_bps: i32,
    pub chase_ceiling: Price,
    pub band_floor: Price,
    pub band_ceiling_exclusive: Price,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservationEvidence {
    pub source_receipt: AppendReceipt,
    pub complete_bound_receipt: AppendReceipt,
    pub observed_unix_ms: i64,
    pub provenance: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EconomicPrepared {
    pub version: u16,
    pub market: MarketSelection,
    pub admission: LiveAdmissionArtifactAudit,
    pub ladder: LadderPlanAudit,
    pub book_receipt: AppendReceipt,
    pub observation: Option<ObservationEvidence>,
    pub sizing: SizingAudit,
    pub fee: FeeAudit,
    pub risk: RiskAudit,
    pub balance: BalanceAudit,
    pub applied_configuration_hash: String,
}

/// Inputs to the one economic composition shared by paper and live. Every field is evidence the
/// caller already holds; composition performs no I/O and reads no clocks.
pub struct EconomicInputs<'a> {
    pub market: MarketSelection,
    pub admission: &'a LiveAdmissionArtifact,
    pub plan: &'a LadderPlan,
    pub book_receipt: AppendReceipt,
    pub observation: Option<ObservationEvidence>,
    pub sizing_mode: SizingModeAudit,
    pub budget: CollateralAmount,
    pub slippage_rate: Decimal,
    pub risk: RiskAudit,
    pub cash_before: CollateralAmount,
    pub price_impact_cap_bps: i32,
    pub chase_ceiling: Price,
    pub band_floor: Price,
    pub band_ceiling_exclusive: Price,
    pub applied_configuration_hash: String,
}

#[derive(Debug, thiserror::Error)]
pub enum EconomicError {
    #[error("fee economics: {0}")]
    Fee(#[from] FeeError),
    #[error("exact amount arithmetic: {0}")]
    Arithmetic(#[from] pe_core_types::Error),
    #[error("ladder economics: {0}")]
    Ladder(#[from] LadderError),
    #[error("the book append receipt is not bound to a source frame")]
    MissingBookReceipt,
}

impl EconomicPrepared {
    /// Compose fee, reserve, all-in price, expected fill, and audit facts exactly once.
    pub fn compose(inputs: EconomicInputs<'_>) -> Result<Self, EconomicError> {
        if inputs.book_receipt.this_hash == blake3::Hash::from_bytes([0; 32]) {
            return Err(EconomicError::MissingBookReceipt);
        }
        let expected_fee = taker_fee(
            inputs.admission.fee_schedule,
            inputs.plan.shares,
            inputs.plan.limit_price,
        )?;
        let reserve = fee_reserve(
            inputs.admission.fee_schedule,
            inputs.plan.worst_case_debit,
            inputs.plan.shares,
            inputs.plan.best_ask,
            inputs.plan.limit_price,
        )?;
        let ladder = LadderPlanAudit::new(inputs.plan);
        let expected_shares = ladder.expected_shares()?;
        let expected_vwap = ladder.expected_vwap().ok_or(LadderError::Amount)?;
        let principal_plus_fee = inputs.plan.worst_case_debit.checked_add(expected_fee)?;
        let all_in_decimal = principal_plus_fee
            .to_decimal()
            .checked_div(inputs.plan.shares.to_decimal())
            .and_then(|price| {
                Decimal::ONE
                    .checked_add(inputs.slippage_rate)
                    .and_then(|multiplier| price.checked_mul(multiplier))
            })
            .ok_or(pe_core_types::Error::OutOfRange {
                field: "EconomicPrepared.all_in_price",
            })?;
        let all_in_price = Price::new(all_in_decimal)?;
        let worst_case_debit = inputs.plan.worst_case_debit.checked_add(reserve)?;
        Ok(Self {
            version: ECONOMIC_PREPARED_VERSION,
            market: inputs.market,
            admission: LiveAdmissionArtifactAudit::new(
                &inputs.admission.market,
                &inputs.admission.settlement,
                inputs.admission.fee_schedule,
                inputs.admission.receipts,
            ),
            ladder,
            book_receipt: inputs.book_receipt,
            observation: inputs.observation,
            sizing: SizingAudit {
                mode: inputs.sizing_mode,
                budget: inputs.budget,
                principal: inputs.plan.worst_case_debit,
                minimum_shares: inputs.plan.shares,
                expected_shares,
                expected_vwap,
                all_in_price,
                slippage_rate: inputs.slippage_rate,
            },
            fee: FeeAudit {
                schedule: inputs.admission.fee_schedule,
                expected_fee,
                reserve,
            },
            risk: inputs.risk,
            balance: BalanceAudit {
                cash_before: inputs.cash_before,
                worst_case_debit,
                price_impact_cap_bps: inputs.price_impact_cap_bps,
                chase_ceiling: inputs.chase_ceiling,
                band_floor: inputs.band_floor,
                band_ceiling_exclusive: inputs.band_ceiling_exclusive,
            },
            applied_configuration_hash: inputs.applied_configuration_hash,
        })
    }

    /// Deterministic BLAKE3 over the domain, version, and canonical JSON record.
    pub fn core_hash(&self) -> Result<String, LiveJournalError> {
        hash_serializable(&("prediction-edge/economic-prepared", self.version, self))
    }

    pub fn all_in_debit(&self) -> Result<CollateralAmount, pe_core_types::Error> {
        self.sizing.principal.checked_add(self.fee.expected_fee)
    }

    pub fn worst_case_all_in_debit(&self) -> Result<CollateralAmount, pe_core_types::Error> {
        self.sizing.principal.checked_add(self.fee.reserve)
    }
}
